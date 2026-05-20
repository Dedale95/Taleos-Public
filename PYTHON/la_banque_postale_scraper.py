#!/usr/bin/env python3
"""
LA BANQUE POSTALE — JOB SCRAPER
Extrait les offres d'emploi depuis le site labanquepostale.com.
100% requêtes HTTP — pas de Playwright nécessaire.

Architecture AEM (Adobe Experience Manager) :
  Listing : /candidats/offres-d-emploi/nos-offres-d-emploi/_jcr_content/jobofferlist.p-{N}.html
  Détail  : /candidats/offres-d-emploi/nos-offres-d-emploi.job-{ID}.html/{slug}.html
  4 offres par page.
"""

import logging
import re
import sqlite3
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Set
from html import unescape

import requests
from bs4 import BeautifulSoup

try:
    from job_family_classifier import classify_job_family
    from experience_extractor import extract_experience_level
    from country_normalizer import normalize_country
except ImportError:
    import sys
    sys.path.append(str(Path(__file__).parent))
    from job_family_classifier import classify_job_family
    from experience_extractor import extract_experience_level
    from country_normalizer import normalize_country

# ─────────────────────────── Logging ────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# ─────────────────────────── Constants ──────────────────────────
BASE_URL      = "https://www.labanquepostale.com"
LISTING_API   = (
    BASE_URL
    + "/candidats/offres-d-emploi/nos-offres-d-emploi"
    + "/_jcr_content/jobofferlist.p-{page}.html"
)
COMPANY_NAME  = "La Banque Postale"

JOBS_PER_PAGE  = 4
REQUEST_DELAY  = 1.0    # secondes entre les requêtes listing
DETAIL_DELAY   = 0.8    # secondes entre les pages détail
REQUEST_TIMEOUT = 30
MAX_RETRIES    = 3

# ─────────────── Filiales publiques LBP ────────────────────────
# Les entités renvoyées par le site sont souvent des directions internes
# (DSI, DFI, DRH…). On normalise vers les marques / entités publiques connues.
# Tout ce qui ne match pas → "La Banque Postale".
LBP_SUBSIDIARY_PATTERNS: Dict[str, str] = {
    "leasing":              "La Banque Postale Leasing & Factoring",
    "factoring":            "La Banque Postale Leasing & Factoring",
    "assurance":            "La Banque Postale Assurances",
    "bedl":                 "BEDL",
    "kkbb":                 "KKBB",
    "sofiap":               "Sofiap",
    "vivier":               "Vivier",
    "easy bourse":          "Easybourse",
    "easybourse":           "Easybourse",
    "filbanque":            "Filbanque",
    "la banque postale sa": "La Banque Postale",
}

def normalize_lbp_entity(raw: str) -> str:
    """Normalise une entité LBP : filiale publique ou 'La Banque Postale'."""
    if not raw:
        return COMPANY_NAME
    n = raw.strip().lower()
    for key, value in LBP_SUBSIDIARY_PATTERNS.items():
        if key in n:
            return value
    return COMPANY_NAME


HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,*/*",
    "Accept-Language": "fr-FR,fr;q=0.9",
}

# ─────────────── Mapping contrat → Taleos ───────────────────────
CONTRACT_MAP: Dict[str, str] = {
    "cdi":        "CDI",
    "cdd":        "CDD",
    "alternance": "Alternance",
    "stage":      "Stage",
}

# ─────────────── Mapping niveau études ──────────────────────────
EDUCATION_MAP: Dict[str, str] = {
    "bac":         "Bac",
    "bac + 2":     "Bac+2",
    "bac +2":      "Bac+2",
    "bac+2":       "Bac+2",
    "bac + 3":     "Bac+3/Licence",
    "bac +3":      "Bac+3/Licence",
    "bac+3":       "Bac+3/Licence",
    "bac + 4":     "Bac+4/Master 1",
    "bac +4":      "Bac+4/Master 1",
    "bac+4":       "Bac+4/Master 1",
    "bac + 5":     "Bac+5/Master 2",
    "bac +5":      "Bac+5/Master 2",
    "bac+5":       "Bac+5/Master 2",
    "master":      "Bac+5/Master 2",
    "grande ecole":"Bac+5/Master 2",
    "grande école":"Bac+5/Master 2",
    "doctorat":    "Doctorat/PhD",
    "phd":         "Doctorat/PhD",
}

# ─────────────── Mapping expérience ─────────────────────────────
EXPERIENCE_MAP: Dict[str, str] = {
    "de 0 à 2 ans":   "0 - 2 ans",
    "de 3 à 5 ans":   "3 - 5 ans",
    "de 6 à 10 ans":  "6 - 10 ans",
    "de 11 à 20 ans": "11 ans et plus",
    "plus de 20 ans": "11 ans et plus",
}

# ─────────────── Config ──────────────────────────────────────────
class Config:
    BASE_DIR = Path(__file__).parent
    DB_PATH  = BASE_DIR / "la_banque_postale_jobs.db"


# ═══════════════════════════════════════════════════════════════
#  DATABASE
# ═══════════════════════════════════════════════════════════════
class Database:
    def __init__(self, db_path: Path):
        self.db_path = db_path
        self._init()

    def _init(self):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS jobs (
                    job_url              TEXT PRIMARY KEY,
                    job_id               TEXT,
                    job_title            TEXT,
                    contract_type        TEXT,
                    publication_date     TEXT,
                    location             TEXT,
                    job_family           TEXT,
                    duration             TEXT,
                    management_position  INTEGER DEFAULT 0,
                    status               TEXT DEFAULT 'Live',
                    company_name         TEXT,
                    job_description      TEXT,
                    experience_level     TEXT,
                    education_level      TEXT,
                    country              TEXT,
                    region               TEXT,
                    source               TEXT DEFAULT 'La Banque Postale',
                    first_seen           TEXT,
                    last_updated         TEXT,
                    is_valid             INTEGER DEFAULT 1
                )
            """)
            conn.commit()

    def get_live_urls(self) -> Set[str]:
        with sqlite3.connect(self.db_path) as conn:
            rows = conn.execute(
                "SELECT job_url FROM jobs WHERE status = 'Live' AND is_valid = 1"
            ).fetchall()
        return {r[0] for r in rows}

    def get_all_urls(self) -> Set[str]:
        with sqlite3.connect(self.db_path) as conn:
            rows = conn.execute("SELECT job_url FROM jobs").fetchall()
        return {r[0] for r in rows}

    def expire_missing(self, current_urls: Set[str]):
        live = self.get_live_urls()
        to_expire = live - current_urls
        if not to_expire:
            return
        placeholders = ",".join("?" * len(to_expire))
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                f"UPDATE jobs SET status = 'Expired', last_updated = CURRENT_TIMESTAMP "
                f"WHERE job_url IN ({placeholders})",
                list(to_expire),
            )
            conn.commit()
        logger.info(f"  ⚰️  {len(to_expire)} offre(s) expirée(s)")

    def upsert(self, job: Dict):
        url = job.get("job_url", "")
        if not url:
            return
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                INSERT INTO jobs (
                    job_url, job_id, job_title, contract_type, publication_date,
                    location, job_family, duration, management_position, status,
                    company_name, job_description, experience_level, education_level,
                    country, region, source, first_seen, last_updated, is_valid
                ) VALUES (
                    :job_url, :job_id, :job_title, :contract_type, :publication_date,
                    :location, :job_family, :duration, :management_position, :status,
                    :company_name, :job_description, :experience_level, :education_level,
                    :country, :region, :source, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, :is_valid
                )
                ON CONFLICT(job_url) DO UPDATE SET
                    job_title        = excluded.job_title,
                    contract_type    = excluded.contract_type,
                    location         = excluded.location,
                    job_family       = excluded.job_family,
                    company_name     = excluded.company_name,
                    experience_level = excluded.experience_level,
                    education_level  = excluded.education_level,
                    country          = excluded.country,
                    region           = excluded.region,
                    status           = excluded.status,
                    last_updated     = CURRENT_TIMESTAMP,
                    is_valid         = excluded.is_valid
            """, {
                "job_url":           url,
                "job_id":            job.get("job_id", ""),
                "job_title":         job.get("job_title", ""),
                "contract_type":     job.get("contract_type", ""),
                "publication_date":  job.get("publication_date", ""),
                "location":          job.get("location", ""),
                "job_family":        job.get("job_family", ""),
                "duration":          job.get("duration", ""),
                "management_position": int(job.get("management_position", 0)),
                "status":            "Live",
                "company_name":      job.get("company_name", COMPANY_NAME),
                "job_description":   job.get("job_description", ""),
                "experience_level":  job.get("experience_level", ""),
                "education_level":   job.get("education_level", ""),
                "country":           job.get("country", "France"),
                "region":            job.get("region", ""),
                "source":            "La Banque Postale",
                "is_valid":          1,
            })
            conn.commit()

    def count_live(self) -> int:
        with sqlite3.connect(self.db_path) as conn:
            return conn.execute(
                "SELECT COUNT(*) FROM jobs WHERE status = 'Live' AND is_valid = 1"
            ).fetchone()[0]


# ═══════════════════════════════════════════════════════════════
#  HTTP
# ═══════════════════════════════════════════════════════════════
def fetch(url: str, session: requests.Session, retries: int = MAX_RETRIES) -> Optional[str]:
    for attempt in range(retries):
        try:
            resp = session.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
            return resp.text
        except Exception as exc:
            logger.warning(f"  ⚠️  Tentative {attempt+1}/{retries} — {exc} — {url}")
            if attempt < retries - 1:
                time.sleep(2 ** attempt)
    return None


# ═══════════════════════════════════════════════════════════════
#  PARSING
# ═══════════════════════════════════════════════════════════════
def parse_contract(raw: str) -> str:
    key = raw.strip().lower()
    return CONTRACT_MAP.get(key, raw.strip())


def parse_pub_date(raw: str) -> str:
    """Convertit 'Il y a N jours/semaines/mois' en date ISO."""
    raw = raw.strip().lower()
    today = datetime.utcnow()
    m = re.search(r'il y a (\d+)\s+(jour|jours|semaine|semaines|mois)', raw)
    if m:
        n = int(m.group(1))
        unit = m.group(2)
        if 'jour' in unit:
            return (today - timedelta(days=n)).strftime("%Y-%m-%d")
        if 'semaine' in unit:
            return (today - timedelta(weeks=n)).strftime("%Y-%m-%d")
        if 'mois' in unit:
            return (today - timedelta(days=n * 30)).strftime("%Y-%m-%d")
    return today.strftime("%Y-%m-%d")


def parse_education(raw: str) -> str:
    key = raw.strip().lower()
    for pattern, value in EDUCATION_MAP.items():
        if pattern in key:
            return value
    return raw.strip()


def clean_text(html_fragment: str) -> str:
    """Supprime les balises HTML et normalise les espaces."""
    text = re.sub(r'<[^>]+>', ' ', html_fragment)
    text = unescape(text)
    text = re.sub(r'[ \t]+', ' ', text)
    text = re.sub(r'\n{3,}', '\n\n', text)
    return text.strip()


def parse_listing_page(html: str) -> List[Dict]:
    """Extrait les offres d'une page de listing (4 offres)."""
    soup = BeautifulSoup(html, "html.parser")
    cards = soup.find_all("div", class_=lambda c: c and "o-jobOffer__push" in c and "js-has-link" in c)
    jobs = []
    for card in cards:
        # Titre
        h3 = card.find("h3")
        title = h3.get_text(strip=True) if h3 else ""

        # Type de contrat (tag)
        contract_raw = ""
        for tag_span in card.find_all("span", class_=lambda c: c and "a-cat-tag" in c):
            sr = tag_span.find("span", class_="sr-only")
            if sr and "contrat" in sr.get_text(strip=True).lower():
                val = tag_span.find_all("span")[-1]
                contract_raw = val.get_text(strip=True)
                break

        # Infos (ville + entité)
        infos_p = card.find("p", class_="o-jobOffer__push__infos")
        info_spans = infos_p.find_all("span") if infos_p else []
        location_raw = info_spans[0].get_text(strip=True) if len(info_spans) > 0 else ""
        entity       = info_spans[1].get_text(strip=True) if len(info_spans) > 1 else COMPANY_NAME
        # Filtrer les noms de direction/entité bancaire extraits à tort comme localisation.
        # Ex: "DIRECTION GENERALE DE LA BANQUE DE DETAIL", "DRH DE LA BRANCHE…",
        #     "La Banque Postale SA", "(BEDL) DIRECTION DU DEVELOPPEMENT COMMERCIAL"
        # La page détail fournira la vraie ville via le champ "Ville" de la sidebar.
        _LBP_JUNK_KW = (
            'direction ', 'drh ', 'secrétariat ', 'secretariat ',
            'leasing & factoring', 'branche la banque', ' branche ',
            'banque postale sa', 'la banque postale s',
        )
        _loc_lw = location_raw.lower()
        if (any(kw in _loc_lw for kw in _LBP_JUNK_KW)
                or location_raw.startswith('(')
                or (location_raw == location_raw.upper() and len(location_raw) > 8
                    and location_raw.replace(' ', '').replace("'", '').isalpha())):
            location = ""   # Sera rempli par la page détail (champ "Ville") si disponible
        else:
            location = location_raw

        # URL
        link = card.find("a", attrs={"data-internal": "true"})
        href = link.get("href", "") if link else ""
        if href and not href.startswith("http"):
            href = BASE_URL + href

        # Job ID
        job_id = ""
        m = re.search(r'job-(\d+)', href)
        if m:
            job_id = f"LBP_{m.group(1)}"

        if not href or not title:
            continue

        jobs.append({
            "job_url":      href,
            "job_id":       job_id,
            "job_title":    title,
            "contract_type": parse_contract(contract_raw) if contract_raw else "",
            "location":     location,
            "company_name": normalize_lbp_entity(unescape(entity)) if entity else COMPANY_NAME,
        })
    return jobs


def parse_detail_page(html: str, base_job: Dict) -> Dict:
    """Enrichit un job avec les données de sa page détail."""
    soup = BeautifulSoup(html, "html.parser")
    job = dict(base_job)

    # ── Sidebar ──────────────────────────────────────────────
    sidebar = soup.find(class_=lambda c: c and "o-jobOffer-details__sidebar" in c)
    if sidebar:
        sidebar_text = sidebar.get_text(separator="\n", strip=True)
        lines = [l.strip() for l in sidebar_text.split("\n") if l.strip()]

        # Parser les paires label:valeur
        i = 0
        while i < len(lines):
            line = lines[i]
            # Publication date
            if re.search(r'il y a', line, re.IGNORECASE):
                job["publication_date"] = parse_pub_date(line)
            # Contrat
            if line.lower() == "type de contrat :" and i + 1 < len(lines):
                job["contract_type"] = parse_contract(lines[i + 1])
            # Entité
            if line.lower() in ("entité", "entite") and i + 1 < len(lines):
                job["company_name"] = normalize_lbp_entity(unescape(lines[i + 1]))
            # Niveau d'études
            if "niveau" in line.lower() and "étud" in line.lower() and i + 1 < len(lines):
                job["education_level"] = parse_education(lines[i + 1])
            # Région
            if line.lower() == "région" and i + 1 < len(lines):
                job["region"] = lines[i + 1]
            # Ville
            if line.lower() == "ville" and i + 1 < len(lines):
                job["location"] = lines[i + 1]
            # Expérience
            if "expérience" in line.lower() and i + 1 < len(lines):
                raw_exp = lines[i + 1].lower()
                job["experience_level"] = EXPERIENCE_MAP.get(raw_exp, "")
            i += 1

    # ── Description ──────────────────────────────────────────
    content_sections = soup.find_all(
        class_=lambda c: c and "o-jobOffer-details__content" in c
    )
    description_parts = []
    for section in content_sections:
        text = section.get_text(separator="\n", strip=True)
        if text:
            description_parts.append(text)
    full_desc = "\n\n".join(description_parts)
    job["job_description"] = full_desc

    # ── Job family (NLP sur titre + description) ─────────────
    if not job.get("job_family"):
        job["job_family"] = classify_job_family(
            job.get("job_title", ""), full_desc
        )

    # ── Experience level (NLP si pas en sidebar) ─────────────
    if not job.get("experience_level") and full_desc:
        job["experience_level"] = extract_experience_level(
            job.get("job_title", ""), full_desc
        )

    # ── Pays ─────────────────────────────────────────────────
    job.setdefault("country", "France")

    # ── Localisation par défaut ───────────────────────────────
    # La Banque Postale ne recrute qu'en France. Si la ville n'a pas pu
    # être extraite de la liste ni de la page détail, on se replie sur
    # "France" pour éviter que l'offre tombe dans "Non spécifié".
    if not job.get("location"):
        region = job.get("region", "")
        if region and region.lower() not in ("", "france"):
            # La sidebar a fourni une région (ex: "Île-de-France") : on l'utilise
            job["location"] = f"{region} - France"
        else:
            job["location"] = "France"

    return job


# ═══════════════════════════════════════════════════════════════
#  SCRAPER PRINCIPAL
# ═══════════════════════════════════════════════════════════════
def get_total_pages(session: requests.Session) -> int:
    """Lit le nombre total de pages depuis la première page listing."""
    html = fetch(LISTING_API.format(page=1), session)
    if not html:
        return 0
    m = re.search(r'data-last-page="(\d+)"', html)
    if m:
        return int(m.group(1))
    # Fallback : titre "Page 1 sur N"
    m2 = re.search(r'Page \d+ sur (\d+)', html)
    return int(m2.group(1)) if m2 else 1


def scrape_all_listing_pages(session: requests.Session, total_pages: int) -> List[Dict]:
    """Parcourt toutes les pages listing et retourne la liste de toutes les offres."""
    all_jobs: List[Dict] = []
    seen_urls: Set[str] = set()

    for page in range(1, total_pages + 1):
        url = LISTING_API.format(page=page)
        logger.info(f"  📄 Page {page}/{total_pages} — {url}")
        html = fetch(url, session)
        if not html:
            logger.warning(f"  ⚠️  Page {page} inaccessible, on continue")
            time.sleep(REQUEST_DELAY)
            continue

        jobs = parse_listing_page(html)
        new = [j for j in jobs if j["job_url"] not in seen_urls]
        for j in new:
            seen_urls.add(j["job_url"])
        all_jobs.extend(new)

        logger.info(f"      → {len(new)} offre(s) récupérée(s) (total={len(all_jobs)})")
        time.sleep(REQUEST_DELAY)

    return all_jobs


def enrich_with_detail(jobs: List[Dict], existing_urls: Set[str], session: requests.Session) -> List[Dict]:
    """Enrichit les nouvelles offres avec le contenu des pages détail."""
    enriched = []
    to_fetch = [j for j in jobs if j["job_url"] not in existing_urls]
    already  = [j for j in jobs if j["job_url"]     in existing_urls]

    logger.info(f"  🔍 {len(to_fetch)} nouvelles offres à enrichir / {len(already)} déjà en base")

    for idx, job in enumerate(to_fetch, 1):
        url = job["job_url"]
        logger.info(f"  [{idx}/{len(to_fetch)}] Détail: {url}")
        html = fetch(url, session)
        if html:
            enriched.append(parse_detail_page(html, job))
        else:
            logger.warning(f"  ⚠️  Détail inaccessible: {url}")
            job.setdefault("job_family", classify_job_family(job.get("job_title", ""), ""))
            enriched.append(job)
        time.sleep(DETAIL_DELAY)

    return enriched + already


# ═══════════════════════════════════════════════════════════════
#  ENTRY POINT
# ═══════════════════════════════════════════════════════════════
def main():
    logger.info("🏦 Démarrage scraper La Banque Postale")
    db      = Database(Config.DB_PATH)
    session = requests.Session()
    session.headers.update(HEADERS)

    # 1. Nombre de pages
    total_pages = get_total_pages(session)
    logger.info(f"  📊 {total_pages} pages de listing détectées")
    if total_pages == 0:
        logger.error("  ❌ Impossible de lire le nombre de pages")
        return

    # 2. Lister toutes les offres en ligne
    all_jobs = scrape_all_listing_pages(session, total_pages)
    current_urls = {j["job_url"] for j in all_jobs}
    logger.info(f"  ✅ {len(all_jobs)} offres live sur le site")

    # 3. Marquer expirées
    db.expire_missing(current_urls)

    # 4. Enrichir les nouvelles avec les pages détail
    existing_urls = db.get_all_urls()
    enriched = enrich_with_detail(all_jobs, existing_urls, session)

    # 5. Upsert
    for job in enriched:
        db.upsert(job)

    logger.info(f"  💾 {db.count_live()} offres live en base")
    logger.info("✅ Scraping La Banque Postale terminé")


if __name__ == "__main__":
    main()
