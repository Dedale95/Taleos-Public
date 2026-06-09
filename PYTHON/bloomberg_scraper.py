#!/usr/bin/env python3
"""
Bloomberg — Job Scraper
========================
Source : Avature ATS (bloomberg.avature.net/careers)
  Listing : GET /careers/SearchJobs/?listFilterMode=1&jobRecordsPerPage=12&jobOffset={n}
  Détail  : GET /careers/JobDetail/{slug}/{id}
  Total   : ~478 offres (professional + early careers dans le même portail)

Structure :
  listing : title, location, job URL, job ID (from URL)
  détail  : title, location, business_area, ref_number, description

Delta scraping : job_id (numérique Avature) comme clé unique
                 → listing toujours fetché (HTML paginé)
                 → détail uniquement pour nouvelles offres
"""
import json
import logging
import re
import sqlite3
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import requests
from bs4 import BeautifulSoup

try:
    from country_normalizer import normalize_country
    from job_family_classifier import classify_job_family
    from experience_extractor import extract_experience_level
    from education_extractor import extract_education_level
except ImportError:
    sys.path.append(str(Path(__file__).parent))
    from country_normalizer import normalize_country
    from job_family_classifier import classify_job_family
    from experience_extractor import extract_experience_level
    from education_extractor import extract_education_level

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("bloomberg_scraper.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger(__name__)

# ─── Config ──────────────────────────────────────────────────────────────────
BASE_URL      = "https://bloomberg.avature.net/careers"
SEARCH_URL    = f"{BASE_URL}/SearchJobs/"
DETAIL_URL    = f"{BASE_URL}/JobDetail/{{slug}}/{{job_id}}"
DB_PATH       = Path(__file__).parent / "bloomberg_jobs.db"
PAGE_SIZE     = 12          # Avature limite à 12 résultats/page
REQUEST_PAUSE = 0.5         # secondes entre requêtes

COMPANY_NAME  = "Bloomberg"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

# ─── Contract type detection ─────────────────────────────────────────────────
_CONTRACT_PATTERNS = [
    (re.compile(r"\binternship\b|\bintern\b|\bstage\b|\bstagiaire\b", re.I), "Stage"),
    (re.compile(r"\balternance\b|\bapprentice\b|\bapprentissage\b", re.I), "Alternance"),
    (re.compile(r"\bv\.?i\.?e\.?\b|\bvolontariat\s+international\b", re.I), "V.I.E."),
    (re.compile(r"\bfixed.term\b|\btemporary\b|\bcdd\b|\bcontract\b", re.I), "CDD"),
    (re.compile(r"\bpart.time\b|\bmi.temps\b", re.I), "Temps partiel"),
    (re.compile(r"\bco.op\b|\bcooperation\b", re.I), "Stage"),
    (re.compile(r"\bgraduate\b|\bnew.grad\b|\bentry.level\b|\bearly.career\b", re.I), "CDI"),
]


def _detect_contract(title: str, description: str = "") -> str:
    combined = f"{title} {description[:500]}"
    for pat, ctype in _CONTRACT_PATTERNS:
        if pat.search(combined):
            return ctype
    return "CDI"


# ─── Base de données ──────────────────────────────────────────────────────────
def init_db(conn: sqlite3.Connection):
    conn.execute("""
    CREATE TABLE IF NOT EXISTS jobs (
        job_url          TEXT PRIMARY KEY,
        job_id           TEXT UNIQUE,
        job_title        TEXT,
        contract_type    TEXT,
        publication_date TEXT,
        location         TEXT,
        city             TEXT,
        country          TEXT,
        business_area    TEXT,
        ref_number       TEXT,
        job_family       TEXT,
        experience_level TEXT,
        education_level  TEXT,
        job_description  TEXT,
        company_name     TEXT DEFAULT 'Bloomberg',
        status           TEXT DEFAULT 'Live',
        is_valid         INTEGER DEFAULT 1,
        first_seen       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        last_updated     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )""")
    # Migrations pour colonnes éventuellement manquantes
    existing = {r[1] for r in conn.execute("PRAGMA table_info(jobs)")}
    for col, definition in [
        ("business_area", "TEXT"),
        ("ref_number", "TEXT"),
        ("city", "TEXT"),
    ]:
        if col not in existing:
            conn.execute(f"ALTER TABLE jobs ADD COLUMN {col} {definition}")
    conn.commit()


def upsert_job(conn: sqlite3.Connection, row: dict):
    conn.execute("""
    INSERT INTO jobs (
        job_url, job_id, job_title, contract_type, publication_date,
        location, city, country, business_area, ref_number,
        job_family, experience_level, education_level, job_description,
        company_name, status, is_valid, first_seen, last_updated
    ) VALUES (
        :job_url, :job_id, :job_title, :contract_type, :publication_date,
        :location, :city, :country, :business_area, :ref_number,
        :job_family, :experience_level, :education_level, :job_description,
        :company_name, :status, 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
    )
    ON CONFLICT(job_id) DO UPDATE SET
        job_title        = excluded.job_title,
        contract_type    = excluded.contract_type,
        location         = excluded.location,
        city             = excluded.city,
        country          = excluded.country,
        business_area    = excluded.business_area,
        ref_number       = excluded.ref_number,
        job_family       = excluded.job_family,
        experience_level = excluded.experience_level,
        status           = 'Live',
        is_valid         = 1,
        last_updated     = CURRENT_TIMESTAMP,
        job_description  = CASE
            WHEN excluded.job_description IS NOT NULL AND excluded.job_description != ''
            THEN excluded.job_description
            ELSE jobs.job_description
        END,
        education_level  = CASE
            WHEN excluded.education_level IS NOT NULL AND excluded.education_level != ''
            THEN excluded.education_level
            ELSE jobs.education_level
        END
    """, row)


def mark_expired(conn: sqlite3.Connection, live_ids: set):
    count = 0
    for (jid,) in conn.execute("SELECT job_id FROM jobs WHERE is_valid=1"):
        if jid not in live_ids:
            conn.execute(
                "UPDATE jobs SET is_valid=0, status='Expired', last_updated=CURRENT_TIMESTAMP WHERE job_id=?",
                (jid,),
            )
            count += 1
    if count:
        logger.info(f"  ⚠️  {count} offres marquées expirées")
    conn.commit()


def get_ids_without_description(conn: sqlite3.Connection) -> list:
    return [
        (r[0], r[1])
        for r in conn.execute(
            "SELECT job_id, job_url FROM jobs "
            "WHERE is_valid=1 AND (job_description IS NULL OR job_description='')"
        )
    ]


# ─── Scraping ────────────────────────────────────────────────────────────────
def make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update(HEADERS)
    # Initialiser le cookie de session en visitant la page d'accueil
    try:
        s.get(BASE_URL, timeout=15)
    except Exception as exc:
        logger.warning(f"Warm-up session: {exc}")
    return s


def fetch_listing_page(session: requests.Session, offset: int) -> BeautifulSoup:
    params = {
        "listFilterMode": "1",
        "jobRecordsPerPage": str(PAGE_SIZE),
        "jobOffset": str(offset),
    }
    resp = session.get(SEARCH_URL, params=params, timeout=20)
    resp.raise_for_status()
    return BeautifulSoup(resp.text, "html.parser")


def parse_total(soup: BeautifulSoup) -> int:
    """Extrait le nombre total d'offres depuis le HTML de la page de liste."""
    # Avature affiche le total dans le texte de la page, ex: "478 results"
    text = soup.get_text(" ", strip=True)
    match = re.search(r"(\d+)\s+(?:jobs?|results?|positions?)", text, re.I)
    if match:
        return int(match.group(1))
    # Fallback : chercher la dernière page de pagination
    last_offset = 0
    for a in soup.find_all("a", href=lambda h: h and "jobOffset=" in str(h)):
        m = re.search(r"jobOffset=(\d+)", a.get("href", ""))
        if m:
            last_offset = max(last_offset, int(m.group(1)))
    return last_offset + PAGE_SIZE if last_offset else PAGE_SIZE


def parse_jobs_from_list(soup: BeautifulSoup) -> list:
    """Extrait les offres depuis une page de résultats."""
    jobs = []
    for art in soup.select("article.article--result"):
        link = art.find("a", href=lambda h: h and "/careers/JobDetail/" in str(h))
        if not link:
            continue
        href = link.get("href", "")
        # ID = dernier segment numérique de l'URL
        m = re.search(r"/(\d+)$", href)
        if not m:
            continue
        job_id = m.group(1)
        title = link.text.strip()
        # Slug = segment avant l'ID
        slug = href.rstrip("/").rsplit("/", 1)[0].rsplit("/", 1)[-1]
        # Location
        loc_el = art.find(class_="list-item-location")
        location_raw = loc_el.text.strip() if loc_el else ""
        jobs.append({
            "job_id":    job_id,
            "job_url":   href,
            "job_title": title,
            "slug":      slug,
            "location_raw": location_raw,
        })
    return jobs


def fetch_detail(session: requests.Session, job_id: str, job_url: str) -> dict:
    """Récupère la description complète et les métadonnées d'une offre."""
    try:
        resp = session.get(job_url, timeout=20)
        resp.raise_for_status()
    except Exception as exc:
        logger.warning(f"    ⚠️  Détail {job_id}: {exc}")
        return {}

    soup = BeautifulSoup(resp.text, "html.parser")
    result = {}

    for art in soup.find_all("article"):
        classes = " ".join(art.get("class") or [])
        text = art.get_text(" | ", strip=True)

        # Article avec Location / Business Area / Ref #
        if "regular-fields" in classes:
            parts = [p.strip() for p in text.split("|") if p.strip()]
            # Format attendu : "Location | <ville> | Business Area | <area> | Ref # | <ref>"
            for i, part in enumerate(parts):
                if part.lower() == "location" and i + 1 < len(parts):
                    result["location_detail"] = parts[i + 1]
                elif part.lower() == "business area" and i + 1 < len(parts):
                    result["business_area"] = parts[i + 1]
                elif "ref" in part.lower() and i + 1 < len(parts):
                    result["ref_number"] = parts[i + 1]

        # Article de description
        elif "result" not in classes and "actions" not in classes and "view-more" not in classes:
            # Le contenu textuel significatif après "Description & Requirements"
            full_text = art.get_text("\n", strip=True)
            if len(full_text) > 200 and "Description" in full_text:
                # Supprimer l'en-tête "Description & Requirements"
                desc = re.sub(r"^Description\s*[&]\s*Requirements\s*", "", full_text, flags=re.I).strip()
                if desc:
                    result["job_description"] = desc

    return result


def parse_location(location_raw: str) -> tuple:
    """Retourne (city, country) depuis une chaîne comme 'New York, New York, United States of America'."""
    if not location_raw:
        return ("", "")
    parts = [p.strip() for p in location_raw.split(",")]
    city = parts[0] if parts else ""
    country_raw = parts[-1] if len(parts) >= 2 else ""

    # Normaliser le pays
    country_map = {
        "united states of america": "États-Unis",
        "united states": "États-Unis",
        "usa": "États-Unis",
        "united kingdom": "Royaume-Uni",
        "uk": "Royaume-Uni",
        "france": "France",
        "germany": "Allemagne",
        "singapore": "Singapour",
        "hong kong": "Hong Kong",
        "japan": "Japon",
        "australia": "Australie",
        "india": "Inde",
        "canada": "Canada",
        "switzerland": "Suisse",
        "brazil": "Brésil",
        "china": "Chine",
        "uae": "Émirats arabes unis",
        "united arab emirates": "Émirats arabes unis",
        "south africa": "Afrique du Sud",
        "netherlands": "Pays-Bas",
        "sweden": "Suède",
        "spain": "Espagne",
        "italy": "Italie",
        "poland": "Pologne",
        "luxembourg": "Luxembourg",
        "belgium": "Belgique",
        "austria": "Autriche",
        "denmark": "Danemark",
        "norway": "Norvège",
    }
    country = country_map.get(country_raw.lower(), normalize_country(country_raw))
    return (city, country)


# ─── Main ─────────────────────────────────────────────────────────────────────
def run():
    logger.info("=" * 60)
    logger.info("Bloomberg Scraper — démarrage")
    logger.info("=" * 60)

    session = make_session()
    conn = sqlite3.connect(DB_PATH)
    init_db(conn)

    # ── Étape 1 : listing complet ─────────────────────────────────────────────
    logger.info("📋 Récupération du listing…")
    offset = 0
    all_listed_jobs = []
    total = None

    # Première page pour obtenir le total
    try:
        soup0 = fetch_listing_page(session, 0)
        total = parse_total(soup0)
        page_jobs = parse_jobs_from_list(soup0)
        all_listed_jobs.extend(page_jobs)
        logger.info(f"   Page 1 (offset=0) → {len(page_jobs)} offres  (total annoncé: {total})")
    except Exception as exc:
        logger.error(f"Erreur page 1: {exc}")
        conn.close()
        return

    offset = PAGE_SIZE
    while offset < (total or 9999):
        time.sleep(REQUEST_PAUSE)
        try:
            soup = fetch_listing_page(session, offset)
            page_jobs = parse_jobs_from_list(soup)
            if not page_jobs:
                logger.info(f"   offset={offset} → 0 offres, arrêt pagination")
                break
            all_listed_jobs.extend(page_jobs)
            logger.info(f"   offset={offset} → {len(page_jobs)} offres")
        except Exception as exc:
            logger.warning(f"   offset={offset}: {exc}")
            break
        offset += PAGE_SIZE

    logger.info(f"✅ Listing terminé : {len(all_listed_jobs)} offres trouvées")

    # ── Étape 2 : détails pour nouvelles offres ───────────────────────────────
    live_ids = {j["job_id"] for j in all_listed_jobs}
    ids_without_desc = {jid for jid, _ in get_ids_without_description(conn)}

    need_detail = [
        j for j in all_listed_jobs
        if j["job_id"] not in {
            r[0] for r in conn.execute(
                "SELECT job_id FROM jobs WHERE job_description IS NOT NULL AND job_description != '' AND is_valid=1"
            )
        }
    ]

    logger.info(f"📄 Détails à fetcher : {len(need_detail)} nouvelles offres")

    for idx, job in enumerate(need_detail, 1):
        time.sleep(REQUEST_PAUSE)
        detail = fetch_detail(session, job["job_id"], job["job_url"])
        job.update(detail)
        if idx % 50 == 0:
            logger.info(f"   …{idx}/{len(need_detail)} détails récupérés")

    logger.info("✅ Détails terminés")

    # ── Étape 3 : upsert en base ──────────────────────────────────────────────
    logger.info("💾 Upsert en base…")
    for job in all_listed_jobs:
        location_raw = job.get("location_detail") or job.get("location_raw") or ""
        city, country = parse_location(location_raw)

        title       = job.get("job_title", "")
        description = job.get("job_description", "")
        contract    = _detect_contract(title, description)
        job_family  = classify_job_family(title, description)
        exp_level   = extract_experience_level(title, description)
        edu_level   = extract_education_level(description) if description else ""

        # Location compacte "Ville, Pays"
        if city and country:
            location_display = f"{city}, {country}"
        elif country:
            location_display = country
        else:
            location_display = location_raw

        row = {
            "job_url":          job["job_url"],
            "job_id":           job["job_id"],
            "job_title":        title,
            "contract_type":    contract,
            "publication_date": "",   # non disponible dans Avature sans authentification
            "location":         location_display,
            "city":             city,
            "country":          country,
            "business_area":    job.get("business_area", ""),
            "ref_number":       job.get("ref_number", ""),
            "job_family":       job_family,
            "experience_level": exp_level,
            "education_level":  edu_level,
            "job_description":  description,
            "company_name":     COMPANY_NAME,
            "status":           "Live",
        }
        upsert_job(conn, row)
    conn.commit()
    logger.info(f"✅ {len(all_listed_jobs)} offres upsertées")

    # ── Étape 4 : marquer les offres expirées ─────────────────────────────────
    mark_expired(conn, live_ids)

    # ── Résumé ────────────────────────────────────────────────────────────────
    total_live = conn.execute("SELECT COUNT(*) FROM jobs WHERE is_valid=1").fetchone()[0]
    total_all  = conn.execute("SELECT COUNT(*) FROM jobs").fetchone()[0]
    logger.info("=" * 60)
    logger.info(f"Bloomberg : {total_live} offres live / {total_all} total en DB")
    logger.info("=" * 60)

    conn.close()


if __name__ == "__main__":
    run()
