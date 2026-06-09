#!/usr/bin/env python3
"""
Rothschild & Co — Job Scraper
================================
Source : Site carrières Rothschild & Co (CMS Episerver/Optimizely)
  Professionnels : GET https://www.rothschildandco.com/en/careers/experienced-professionals/vacancies/?page={n}
  Étudiants/grads : GET https://www.rothschildandco.com/en/careers/students-and-graduates/opportunities/?page={n}
  Détail offre    : GET https://www.rothschildandco.com{slug}

Structure HTML :
  listing : li.filterFormGridItem > div.promoBlock > a[href], h3, p.subTitle, p.subTitleLoc
  détail  : main (titre, localisation, type contrat, description)

Note : WORKDAY NE FONCTIONNE PAS pour Rothschild — leur ATS public est intégré
directement dans leur site CMS, pas dans Workday.

Delta scraping : slug URL comme clé unique (wd_id gardé comme alias pour compat DB)
"""
import logging
import re
import sqlite3
import sys
import time
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
        logging.FileHandler("rothschild_scraper.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger(__name__)

# ─── Config ──────────────────────────────────────────────────────────────────
BASE_URL      = "https://www.rothschildandco.com"
LISTING_URLS  = [
    # (url_base, contract_hint)
    ("/en/careers/experienced-professionals/vacancies/",      None),
    ("/en/careers/students-and-graduates/opportunities/",     None),
]
DB_PATH       = Path(__file__).parent / "rothschild_jobs.db"
REQUEST_PAUSE = 0.5
COMPANY_NAME  = "Rothschild & Co"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,*/*;q=0.9",
    "Accept-Language": "en-GB,en;q=0.9,fr;q=0.8",
}

_CONTRACT_PATTERNS = [
    (re.compile(r"\binternship\b|\bintern\b|\bstage\b|\bstagiaire\b|\blong.term.intern\b", re.I), "Stage"),
    (re.compile(r"\balternance\b|\bapprentice\b|\bapprentissage\b", re.I), "Alternance"),
    (re.compile(r"\bv\.?i\.?e\.?\b|\bvolontariat\s+international\b", re.I), "V.I.E."),
    (re.compile(r"\bfixed.term\b|\bcdd\b|\btemporary\b|\bcontract\b", re.I), "CDD"),
    (re.compile(r"\bpart.time\b|\bmi.temps\b", re.I), "Temps partiel"),
    (re.compile(r"\bgraduate\b|\bnew.grad\b|\bentry.level\b", re.I), "CDI"),
]

LEVEL_MAP = {
    "Managing Director": "11 ans et plus", "MD": "11 ans et plus",
    "Partner": "11 ans et plus", "Principal": "11 ans et plus",
    "Director": "11 ans et plus",
    "Vice President": "6 - 10 ans", "VP": "6 - 10 ans", "Senior": "6 - 10 ans",
    "Associate": "3 - 5 ans", "Manager": "3 - 5 ans",
    "Analyst": "0 - 2 ans", "Intern": "0 - 2 ans", "Graduate": "0 - 2 ans",
    "Trainee": "0 - 2 ans", "Apprentice": "0 - 2 ans",
}

# Niveaux grade tels qu'affichés dans p.subTitleLoc
GRADE_TO_LEVEL = {
    "Assistant Director/Vice President/Principal": "6 - 10 ans",
    "Associate/Manager/Investment Associate": "3 - 5 ans",
    "Analyst/Executive/Investment Analyst": "0 - 2 ans",
    "Director": "11 ans et plus",
    "Business Support/Staff": "3 - 5 ans",
}

COUNTRY_NAMES = {
    "france": "France", "germany": "Allemagne", "united kingdom": "Royaume-Uni",
    "uk": "Royaume-Uni", "united states": "États-Unis", "usa": "États-Unis",
    "united states of america": "États-Unis", "switzerland": "Suisse",
    "luxembourg": "Luxembourg", "spain": "Espagne", "italy": "Italie",
    "singapore": "Singapour", "hong kong": "Hong Kong", "japan": "Japon",
    "australia": "Australie", "canada": "Canada", "uae": "Émirats arabes unis",
    "united arab emirates": "Émirats arabes unis", "monaco": "Monaco",
    "netherlands": "Pays-Bas", "belgium": "Belgique", "austria": "Autriche",
    "india": "Inde", "brazil": "Brésil", "china": "Chine",
}


def _detect_contract(title: str, description: str = "") -> str:
    combined = f"{title} {description[:300]}"
    for pat, ctype in _CONTRACT_PATTERNS:
        if pat.search(combined):
            return ctype
    return "CDI"


def _detect_level(title: str, grade_text: str = "") -> str:
    # Grade direct depuis la page listing
    for grade_key, level in GRADE_TO_LEVEL.items():
        if grade_key.lower() in (grade_text or "").lower():
            return level
    # Depuis le titre
    for keyword, level in LEVEL_MAP.items():
        if re.search(r'\b' + re.escape(keyword) + r'\b', title or "", re.I):
            return level
    return ""


def _parse_location(loc_text: str) -> tuple:
    """
    Extrait (location_display, city, country) depuis des chaînes comme:
    'France | Paris | Analyst/Executive/Investment Analyst'
    'Singapore'
    'United Kingdom | London'
    """
    if not loc_text:
        return ("", "", "")
    parts = [p.strip() for p in loc_text.split("|")]
    country_raw = parts[0] if parts else ""
    city = parts[1] if len(parts) >= 2 else ""
    country = COUNTRY_NAMES.get(country_raw.lower(), normalize_country(country_raw))
    if city and country:
        display = f"{city}, {country}"
    elif country:
        display = country
    else:
        display = loc_text.split("|")[0].strip()
    return (display, city, country)


# ─── Base de données ──────────────────────────────────────────────────────────
def init_db(conn: sqlite3.Connection):
    conn.execute("""
    CREATE TABLE IF NOT EXISTS jobs (
        job_url          TEXT PRIMARY KEY,
        wd_id            TEXT UNIQUE,
        job_title        TEXT,
        contract_type    TEXT,
        publication_date TEXT,
        location         TEXT,
        city             TEXT,
        country          TEXT,
        region           TEXT,
        department       TEXT,
        job_family       TEXT,
        experience_level TEXT,
        education_level  TEXT,
        job_description  TEXT,
        company_name     TEXT DEFAULT 'Rothschild & Co',
        status           TEXT DEFAULT 'Live',
        is_valid         INTEGER DEFAULT 1,
        first_seen       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        last_updated     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )""")
    existing = {r[1] for r in conn.execute("PRAGMA table_info(jobs)")}
    for col, dflt in [("is_valid", "1"), ("city", "''"), ("region", "''")]:
        if col not in existing:
            conn.execute(f"ALTER TABLE jobs ADD COLUMN {col} INTEGER DEFAULT {dflt}")
    conn.commit()


def upsert_job(conn: sqlite3.Connection, row: dict):
    conn.execute("""
    INSERT INTO jobs (
        job_url, wd_id, job_title, contract_type, publication_date,
        location, city, country, region, department, job_family,
        experience_level, education_level, job_description,
        company_name, status, is_valid, first_seen, last_updated
    ) VALUES (
        :job_url, :wd_id, :job_title, :contract_type, :publication_date,
        :location, :city, :country, :region, :department, :job_family,
        :experience_level, :education_level, :job_description,
        :company_name, :status, 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
    )
    ON CONFLICT(wd_id) DO UPDATE SET
        job_title        = excluded.job_title,
        contract_type    = excluded.contract_type,
        location         = excluded.location,
        city             = excluded.city,
        country          = excluded.country,
        department       = excluded.department,
        job_family       = excluded.job_family,
        experience_level = excluded.experience_level,
        status           = 'Live',
        is_valid         = 1,
        last_updated     = CURRENT_TIMESTAMP,
        job_description  = CASE
            WHEN excluded.job_description IS NOT NULL AND excluded.job_description != ''
            THEN excluded.job_description ELSE jobs.job_description END,
        education_level  = CASE
            WHEN excluded.education_level IS NOT NULL AND excluded.education_level != ''
            THEN excluded.education_level ELSE jobs.education_level END
    """, row)


def mark_expired(conn: sqlite3.Connection, live_ids: set):
    count = 0
    for (wid,) in conn.execute("SELECT wd_id FROM jobs WHERE is_valid=1"):
        if wid not in live_ids:
            conn.execute(
                "UPDATE jobs SET is_valid=0, status='Expired', "
                "last_updated=CURRENT_TIMESTAMP WHERE wd_id=?", (wid,)
            )
            count += 1
    if count:
        logger.info(f"  ⚠️  {count} offres marquées expirées")
    conn.commit()


def get_ids_without_description(conn: sqlite3.Connection) -> set:
    return {
        r[0] for r in conn.execute(
            "SELECT wd_id FROM jobs WHERE is_valid=1 "
            "AND (job_description IS NULL OR job_description='')"
        )
    }


# ─── Scraping ────────────────────────────────────────────────────────────────
def make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update(HEADERS)
    return s


def fetch_listing_page(session: requests.Session, path: str, page: int) -> BeautifulSoup:
    url = f"{BASE_URL}{path}"
    params = {"page": str(page)} if page > 1 else {}
    resp = session.get(url, params=params, timeout=20)
    resp.raise_for_status()
    return BeautifulSoup(resp.text, "html.parser")


def parse_jobs_from_page(soup: BeautifulSoup) -> list:
    """Extrait les offres depuis une page listing."""
    jobs = []
    for li in soup.select("li.filterFormGridItem"):
        link = li.select_one("a[href]")
        if not link:
            continue
        slug = link.get("href", "")
        if not slug or not slug.startswith("/en/careers/"):
            continue
        title_el = li.select_one("h3")
        division_el = li.select_one("p.subTitle")
        loc_el = li.select_one("p.subTitleLoc")
        jobs.append({
            "slug":     slug,
            "title":    title_el.text.strip() if title_el else "",
            "division": division_el.text.strip() if division_el else "",
            "loc_text": loc_el.text.strip() if loc_el else "",
        })
    return jobs


def get_last_page(soup: BeautifulSoup) -> int:
    """Retourne le numéro de la dernière page depuis la pagination."""
    last = 1
    for a in soup.select(".paginationNavListItem a[href]"):
        m = re.search(r"[?&]page=(\d+)", a.get("href", ""))
        if m:
            last = max(last, int(m.group(1)))
    return last


def fetch_detail(session: requests.Session, slug: str) -> str:
    """Récupère la description complète d'une offre."""
    try:
        resp = session.get(f"{BASE_URL}{slug}", timeout=20)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")
        main = soup.find("main")
        if not main:
            return ""
        # Exclure le header (h1 + metadata) et garder le corps de la description
        # La description suit généralement "About Us" ou "The Role"
        text = main.get_text("\n", strip=True)
        # Couper le header (titre + champs: Location, Division, etc.)
        # La description commence après les champs structurés
        desc_start = re.search(
            r"(?:About Us|About the Role|About Rothschild|The Role|Job Description"
            r"|Description|Overview|Responsibilities|Your role)",
            text, re.I
        )
        if desc_start:
            return text[desc_start.start():].strip()
        # Sinon retourner le texte complet après les 5 premières lignes (header)
        lines = [l for l in text.split("\n") if l.strip()]
        return "\n".join(lines[5:]).strip() if len(lines) > 5 else text
    except Exception as exc:
        logger.warning(f"    ⚠️  Détail {slug}: {exc}")
        return ""


# ─── Main ─────────────────────────────────────────────────────────────────────
def run():
    t0 = time.time()
    logger.info("=" * 60)
    logger.info(f"{COMPANY_NAME} Scraper — démarrage")
    logger.info("=" * 60)

    session = make_session()
    conn = sqlite3.connect(DB_PATH)
    init_db(conn)

    all_listed = []  # liste de dicts avec slug, title, division, loc_text

    for listing_path, _ in LISTING_URLS:
        logger.info(f"📋 Listing : {listing_path}")
        try:
            soup0 = fetch_listing_page(session, listing_path, 1)
        except Exception as exc:
            logger.error(f"   ❌ Page 1 {listing_path}: {exc}")
            continue

        page_jobs = parse_jobs_from_page(soup0)
        last_page = get_last_page(soup0)
        logger.info(f"   Page 1 → {len(page_jobs)} offres  (dernière page: {last_page})")
        all_listed.extend(page_jobs)

        for page in range(2, last_page + 1):
            time.sleep(REQUEST_PAUSE)
            try:
                soup = fetch_listing_page(session, listing_path, page)
                page_jobs = parse_jobs_from_page(soup)
                if not page_jobs:
                    logger.info(f"   Page {page} → 0 offres, arrêt")
                    break
                all_listed.extend(page_jobs)
                logger.info(f"   Page {page} → {len(page_jobs)} offres")
            except Exception as exc:
                logger.warning(f"   Page {page}: {exc}")
                break

    # Dédoublonner par slug
    seen_slugs = {}
    for job in all_listed:
        seen_slugs[job["slug"]] = job
    all_listed = list(seen_slugs.values())

    logger.info(f"✅ Listing terminé : {len(all_listed)} offres trouvées")

    # ── Détails pour les nouvelles offres ─────────────────────────────────────
    live_slugs = set(seen_slugs.keys())
    ids_without = get_ids_without_description(conn)
    existing_slugs = {r[0] for r in conn.execute("SELECT wd_id FROM jobs WHERE wd_id IS NOT NULL")}

    need_detail = [j for j in all_listed if j["slug"] not in existing_slugs or j["slug"] in ids_without]
    logger.info(f"📄 Détails à fetcher : {len(need_detail)}")

    desc_cache = {}
    for idx, job in enumerate(need_detail, 1):
        time.sleep(REQUEST_PAUSE)
        desc_cache[job["slug"]] = fetch_detail(session, job["slug"])
        if idx % 30 == 0:
            logger.info(f"   …{idx}/{len(need_detail)} détails récupérés")

    logger.info("✅ Détails terminés")

    # ── Upsert ────────────────────────────────────────────────────────────────
    logger.info("💾 Upsert en base…")
    for job in all_listed:
        slug     = job["slug"]
        title    = job["title"]
        division = job["division"]
        loc_text = job["loc_text"]
        description = desc_cache.get(slug, "")

        # Extraire grade depuis loc_text (ex: "France | Paris | Analyst/Executive/...")
        loc_parts = [p.strip() for p in loc_text.split("|")]
        grade_text = loc_parts[2] if len(loc_parts) >= 3 else ""

        location_display, city, country = _parse_location(loc_text)
        contract    = _detect_contract(title, description)
        exp_level   = _detect_level(title, grade_text) or extract_experience_level(title, description)
        edu_level   = extract_education_level(description) if description else ""
        job_family  = classify_job_family(title, description)

        row = {
            "job_url":          f"{BASE_URL}{slug}",
            "wd_id":            slug,
            "job_title":        title,
            "contract_type":    contract,
            "publication_date": "",
            "location":         location_display,
            "city":             city,
            "country":          country,
            "region":           "",
            "department":       division,
            "job_family":       job_family,
            "experience_level": exp_level,
            "education_level":  edu_level,
            "job_description":  description,
            "company_name":     COMPANY_NAME,
            "status":           "Live",
        }
        upsert_job(conn, row)
    conn.commit()
    logger.info(f"✅ {len(all_listed)} offres upsertées")

    mark_expired(conn, live_slugs)

    total_live = conn.execute("SELECT COUNT(*) FROM jobs WHERE is_valid=1").fetchone()[0]
    elapsed = time.time() - t0
    logger.info("=" * 60)
    logger.info(f"{COMPANY_NAME} : {total_live} offres live / durée {elapsed:.1f}s")
    logger.info("=" * 60)
    conn.close()


if __name__ == "__main__":
    run()
