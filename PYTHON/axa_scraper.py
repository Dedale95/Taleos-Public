#!/usr/bin/env python3
"""
AXA Careers Scraper — careers.axa.com
======================================
API REST pure (pas de Playwright) :
  GET https://careers.axa.com/api/jobs?page={n}&limit=100&country=France&internal=false

La description complète est incluse dans la réponse listing → pas de scraping de page détail.
873 offres France / ~9 pages × 100.

Schema DB identique aux autres scrapers Taleos.
"""

import json
import logging
import re
import sqlite3
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Optional

import requests

# ---------------------------------------------------------------------------
# Shared utilities (import optional — graceful fallback)
# ---------------------------------------------------------------------------
try:
    from city_normalizer import normalize_city as _normalize_city
except ImportError:
    _normalize_city = None

try:
    from job_family_classifier import classify_job_family
except ImportError:
    def classify_job_family(title: str, desc: str = "") -> Optional[str]:
        return None

try:
    from experience_extractor import extract_experience_level
except ImportError:
    def extract_experience_level(desc: str, contract: str = "", title: str = "") -> Optional[str]:
        return None

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
class Config:
    API_BASE      = "https://careers.axa.com/api/jobs"
    PAGE_LIMIT    = 100           # max supporté par l'API
    COUNTRY       = "France"      # filtre pays — None = mondial
    SORT_BY       = "relevance"
    REQUEST_DELAY = 0.6           # secondes entre pages
    DB_PATH       = Path(__file__).parent / "axa_jobs.db"
    DETAIL_BASE   = "https://careers.axa.com/careers-home/jobs"
    REQUEST_TIMEOUT = 30


config = Config()

# ---------------------------------------------------------------------------
# Normalisation du type de contrat (EN → Taleos)
# ---------------------------------------------------------------------------
_CONTRACT_MAP = {
    "permanent contract":                             "CDI",
    "contrat permanent":                              "CDI",
    "fixed and short-term / seasonal / vie":          "CDD",
    "contrat à durée déterminée / saisonnier / vie":  "CDD",
    "apprenticeship":                                 "Alternance",
    "apprentissage":                                  "Alternance",
    "internship / placement / working student":       "Stage",
    "stage / alternance / étudiant en entreprise":    "Stage",
    "freelance":                                      "Indépendant / Entrepreneur",
}

def normalize_contract(tags2: list) -> str:
    raw = (tags2[0] if tags2 else "").strip()
    return _CONTRACT_MAP.get(raw.lower(), raw)


# ---------------------------------------------------------------------------
# Entité AXA → company_name Taleos
# ---------------------------------------------------------------------------
# Clé en minuscules → valeur canonique Taleos
_ENTITY_MAP_LOWER = {
    "axa france":                 "AXA France",
    "axa banque":                 "AXA Banque",
    "axa group operations":       "AXA Group Operations",
    "axa partners":               "AXA Partners",
    "axa xl":                     "AXA XL",
    "direct assurance":           "Direct Assurance",
    "gie axa":                    "GIE AXA",
    "juridica":                   "Juridica",
    "mutuelle saint christophe":  "Mutuelle Saint-Christophe",
    "mutuelle saint-christophe":  "Mutuelle Saint-Christophe",
    "axa liabilities managers":   "AXA Liabilities Managers",
    "axa investment managers":    "AXA Investment Managers",
}

def resolve_entity(tags3: list) -> str:
    raw = (tags3[0] if tags3 else "").strip()
    return _ENTITY_MAP_LOWER.get(raw.lower(), raw or "AXA")


# ---------------------------------------------------------------------------
# Nettoyage localisation
# Exemples d'entrée : "89-YONNE", "PARIS", "49-MAINE-ET-LOIRE", "FONTENAY SOUS BOIS"
# ---------------------------------------------------------------------------
# Préfixe département français : "49-", "2A-", "971-", etc.
_DEPT_CODE_RE = re.compile(r"^\d{1,3}[AB]?\s*[-–]\s*", re.IGNORECASE)
_PREPS_FR     = {"de", "du", "des", "le", "la", "les", "en", "et", "sur",
                 "sous", "d", "l", "au", "aux", "par"}

def _title_case_fr(s: str) -> str:
    """
    Title-case français respectant les tirets (noms composés) et prépositions.
    "MAINE-ET-LOIRE" → "Maine-et-Loire"
    "FONTENAY SOUS BOIS" → "Fontenay-sous-Bois"
    """
    # On travaille mot par mot en préservant les séparateurs
    def cap_word(w: str, first: bool) -> str:
        return w.lower() if (not first and w.lower() in _PREPS_FR) else w.capitalize()

    # Traitement des segments séparés par tirets
    def cap_segment(seg: str, first_seg: bool) -> str:
        words = seg.split()
        return " ".join(
            cap_word(w, first_seg and i == 0) for i, w in enumerate(words)
        )

    segments = s.split("-")
    result = "-".join(
        cap_segment(seg, i == 0) for i, seg in enumerate(segments)
    )
    return result.strip()

def normalize_location(city_raw: str, country: str = "France") -> str:
    """
    "89-YONNE"         → "Yonne - France"
    "49-MAINE-ET-LOIRE"→ "Maine-et-Loire - France"
    "PARIS"            → "Paris - France"
    "FONTENAY SOUS BOIS"→"Fontenay-sous-Bois - France"
    """
    if not city_raw:
        return country or ""
    # 1. Retire le préfixe département numérique (ex: "49-", "2A-")
    city = _DEPT_CODE_RE.sub("", city_raw).strip()
    # 2. Normalise la casse (normalizer ou fallback)
    if _normalize_city:
        normalized = _normalize_city(city)
        city = normalized if normalized else _title_case_fr(city)
    else:
        city = _title_case_fr(city)
    if country and country.lower() != "france":
        return f"{city} - {country}"
    return f"{city} - France" if city else "France"


# ---------------------------------------------------------------------------
# Strip HTML
# ---------------------------------------------------------------------------
_TAG_RE = re.compile(r"<[^>]+>")
_WS_RE  = re.compile(r"\s+")

def strip_html(html: str) -> str:
    if not html:
        return ""
    text = _TAG_RE.sub(" ", html)
    return _WS_RE.sub(" ", text).strip()


# ---------------------------------------------------------------------------
# Publication date
# ---------------------------------------------------------------------------
def parse_date(raw: str) -> str:
    if not raw:
        return datetime.now().strftime("%Y-%m-%d")
    try:
        # Format attendu : "2026-05-04T10:00:00+0000"
        normalized = raw.replace("+0000", "+00:00").replace("Z", "+00:00")
        return datetime.fromisoformat(normalized).strftime("%Y-%m-%d")
    except Exception:
        m = re.search(r"(\d{4}-\d{2}-\d{2})", raw)
        return m.group(1) if m else datetime.now().strftime("%Y-%m-%d")


# ---------------------------------------------------------------------------
# API
# ---------------------------------------------------------------------------
def _build_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        "Accept": "application/json, text/plain, */*",
        "Referer": "https://careers.axa.com/careers-home/jobs",
    })
    return s


def fetch_page(session: requests.Session, page: int) -> dict:
    params: dict = {
        "page": page,
        "sortBy": config.SORT_BY,
        "descending": "false",
        "internal": "false",
        "limit": config.PAGE_LIMIT,
    }
    if config.COUNTRY:
        params["country"] = config.COUNTRY
    resp = session.get(config.API_BASE, params=params, timeout=config.REQUEST_TIMEOUT)
    resp.raise_for_status()
    return resp.json()


def fetch_all_jobs() -> list[dict]:
    session = _build_session()
    all_items: list[dict] = []

    logger.info("📡 Page 1 …")
    data = fetch_page(session, 1)
    total = data.get("totalCount", 0)
    items = data.get("jobs", [])
    all_items.extend(items)
    logger.info(f"   → {len(items)} offres | total annoncé : {total}")

    total_pages = (total + config.PAGE_LIMIT - 1) // config.PAGE_LIMIT
    logger.info(f"   → {total_pages} pages à parcourir")

    for page in range(2, total_pages + 1):
        time.sleep(config.REQUEST_DELAY)
        logger.info(f"📡 Page {page}/{total_pages} …")
        try:
            data = fetch_page(session, page)
        except Exception as exc:
            logger.warning(f"   ⚠️ Erreur page {page}: {exc} — on continue")
            continue
        items = data.get("jobs", [])
        if not items:
            logger.info("   → 0 résultats, arrêt pagination")
            break
        all_items.extend(items)
        logger.info(f"   → {len(items)} offres (cumul : {len(all_items)})")

    return all_items


# ---------------------------------------------------------------------------
# Transform
# ---------------------------------------------------------------------------
def transform(item: dict) -> dict:
    job = item.get("data", item)

    req_id    = str(job.get("req_id") or job.get("slug") or "")
    job_url   = f"{config.DETAIL_BASE}/{req_id}"
    job_title = (job.get("title") or "").strip()

    tags1 = job.get("tags1") or []   # schedule (Full-time…)
    tags2 = job.get("tags2") or []   # contract type
    tags3 = job.get("tags3") or []   # entity

    contract_type = normalize_contract(tags2)
    company_name  = resolve_entity(tags3)

    city_raw  = job.get("city") or ""
    country   = job.get("country") or ""
    location  = normalize_location(city_raw, country)

    # Description (HTML fourni dans l'API listing)
    desc_html    = job.get("description") or ""
    job_desc_txt = strip_html(desc_html)

    # Job family : classifier NLP en priorité, sinon catégorie API
    raw_cat   = ((job.get("category") or job.get("categories") or [""])[0]).strip()
    job_family = classify_job_family(job_title, job_desc_txt) or raw_cat or None

    # Expérience
    experience_level = extract_experience_level(job_desc_txt, contract_type, job_title)

    pub_date = parse_date(job.get("posted_date") or job.get("create_date") or "")
    now      = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    return {
        "job_url":                job_url,
        "job_id":                 req_id,
        "job_title":              job_title,
        "contract_type":          contract_type,
        "publication_date":       pub_date,
        "location":               location,
        "job_family":             job_family,
        "duration":               None,
        "management_position":    None,
        "status":                 "Live",
        "education_level":        None,
        "experience_level":       experience_level,
        "training_specialization": None,
        "technical_skills":       "[]",
        "behavioral_skills":      "[]",
        "tools":                  None,
        "languages":              None,
        "job_description":        job_desc_txt,
        "company_name":           company_name,
        "company_description":    None,
        "first_seen":             now,
        "last_updated":           now,
    }


# ---------------------------------------------------------------------------
# SQLite DB
# ---------------------------------------------------------------------------
class JobDatabase:
    def __init__(self, db_path: Path):
        self.conn = sqlite3.connect(db_path)
        self._init_schema()

    def _init_schema(self):
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS jobs (
                job_url                 TEXT PRIMARY KEY,
                job_id                  TEXT,
                job_title               TEXT,
                contract_type           TEXT,
                publication_date        TEXT,
                location                TEXT,
                job_family              TEXT,
                duration                TEXT,
                management_position     TEXT,
                status                  TEXT DEFAULT 'Live',
                education_level         TEXT,
                experience_level        TEXT,
                training_specialization TEXT,
                technical_skills        TEXT,
                behavioral_skills       TEXT,
                tools                   TEXT,
                languages               TEXT,
                job_description         TEXT,
                company_name            TEXT,
                company_description     TEXT,
                scrape_attempts         INTEGER DEFAULT 1,
                is_valid                INTEGER DEFAULT 1,
                first_seen              TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_updated            TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        self.conn.commit()

    def get_live_urls(self) -> set[str]:
        rows = self.conn.execute(
            "SELECT job_url FROM jobs WHERE status='Live' AND is_valid=1"
        ).fetchall()
        return {r[0] for r in rows}

    def upsert(self, job: dict):
        is_valid = 1 if job.get("job_title") else 0
        self.conn.execute("""
            INSERT INTO jobs (
                job_url, job_id, job_title, contract_type, publication_date,
                location, job_family, duration, management_position, status,
                education_level, experience_level, training_specialization,
                technical_skills, behavioral_skills, tools, languages,
                job_description, company_name, company_description,
                scrape_attempts, is_valid, first_seen, last_updated
            ) VALUES (
                :job_url, :job_id, :job_title, :contract_type, :publication_date,
                :location, :job_family, :duration, :management_position, :status,
                :education_level, :experience_level, :training_specialization,
                :technical_skills, :behavioral_skills, :tools, :languages,
                :job_description, :company_name, :company_description,
                1, :is_valid, :first_seen, :last_updated
            )
            ON CONFLICT(job_url) DO UPDATE SET
                job_title               = excluded.job_title,
                contract_type           = excluded.contract_type,
                location                = excluded.location,
                job_family              = excluded.job_family,
                status                  = 'Live',
                experience_level        = excluded.experience_level,
                job_description         = excluded.job_description,
                company_name            = excluded.company_name,
                is_valid                = excluded.is_valid,
                last_updated            = excluded.last_updated
        """, {**job, "is_valid": is_valid})

    def mark_expired(self, urls: set[str]):
        if not urls:
            return
        placeholders = ",".join("?" * len(urls))
        self.conn.execute(
            f"UPDATE jobs SET status='Expired', last_updated=CURRENT_TIMESTAMP "
            f"WHERE job_url IN ({placeholders})",
            list(urls),
        )

    def stats(self) -> dict:
        row = self.conn.execute("""
            SELECT
                SUM(CASE WHEN status='Live' AND is_valid=1 THEN 1 ELSE 0 END),
                SUM(CASE WHEN status='Expired'             THEN 1 ELSE 0 END),
                SUM(CASE WHEN is_valid=0                   THEN 1 ELSE 0 END)
            FROM jobs
        """).fetchone()
        return {"live": row[0] or 0, "expired": row[1] or 0, "invalid": row[2] or 0}

    def commit(self):
        self.conn.commit()

    def close(self):
        self.conn.close()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    logger.info("=" * 70)
    logger.info("AXA CAREERS SCRAPER — careers.axa.com")
    logger.info(f"Filtre pays : {config.COUNTRY or 'mondial'}")
    logger.info("=" * 70)

    # 1. Fetch
    logger.info("\n📋 ÉTAPE 1 — Récupération via API REST")
    raw_items = fetch_all_jobs()
    logger.info(f"   → {len(raw_items)} offres brutes récupérées")

    # 2. Transform
    logger.info("\n🔄 ÉTAPE 2 — Transformation")
    jobs: list[dict] = []
    seen_urls: set[str] = set()
    for item in raw_items:
        try:
            job = transform(item)
        except Exception as exc:
            logger.warning(f"   ⚠️ Transformation échouée : {exc}")
            continue
        if job["job_url"] not in seen_urls:
            seen_urls.add(job["job_url"])
            jobs.append(job)
    logger.info(f"   → {len(jobs)} offres uniques transformées")

    # 3. DB upsert + delta
    logger.info("\n💾 ÉTAPE 3 — Mise à jour base SQLite")
    db = JobDatabase(config.DB_PATH)

    existing_live = db.get_live_urls()
    new_urls      = {j["job_url"] for j in jobs}
    expired       = existing_live - new_urls

    for job in jobs:
        db.upsert(job)

    if expired:
        db.mark_expired(expired)
        logger.info(f"   → {len(expired)} offres expirées")

    db.commit()
    s = db.stats()
    logger.info(f"   → Live: {s['live']}  |  Expired: {s['expired']}  |  Invalid: {s['invalid']}")
    db.close()

    logger.info("\n✅ AXA scraper terminé")


if __name__ == "__main__":
    main()
