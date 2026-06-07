#!/usr/bin/env python3
"""
Forvis Mazars — Job Scraper
============================
Source : SmartRecruiters API publique (company slug "mazars")
  API listing  : https://api.smartrecruiters.com/v1/companies/mazars/postings
  API détail   : https://api.smartrecruiters.com/v1/companies/mazars/postings/{id}

Structure API SmartRecruiters :
  listing : id, name, releasedDate, location (city/country/fullLocation),
            typeOfEmployment, experienceLevel, department, function
  détail  : jobAd.sections {companyDescription, jobDescription, qualifications,
            additionalInformation}, postingUrl, applyUrl

Pagination : offset + limit (max 100/page)
Delta scraping : sr_id (SmartRecruiters job ID) comme clé unique
                 → pages listing toujours fetchées
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
    handlers=[logging.StreamHandler(sys.stdout),
              logging.FileHandler("mazars_scraper.log", encoding="utf-8")],
)
logger = logging.getLogger(__name__)

# ─── Config ──────────────────────────────────────────────────────────────────
COMPANY_SLUG  = "mazars"
API_BASE      = f"https://api.smartrecruiters.com/v1/companies/{COMPANY_SLUG}"
DB_PATH       = Path(__file__).parent / "mazars_jobs.db"
PAGE_SIZE     = 100
DETAIL_CAP    = 300   # max nouvelles descriptions par run
REQUEST_PAUSE = 0.3   # secondes entre requêtes

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
    "Accept": "application/json",
}

# ─── Mapping SmartRecruiters typeOfEmployment → contrat Taleos ───────────────
CONTRACT_MAP = {
    "permanent":         "CDI",
    "temporary":         "CDD",
    "part_time":         "Temps partiel",
    "contract":          "CDD",
    "intern":            "Stage",
    "apprentice":        "Alternance",
    "freelance":         "CDI",
    "other":             "CDI",
}

# Patterns titres pour affiner le type de contrat
_CONTRACT_PATTERNS = [
    (re.compile(r"\bstage\b|\bstagiaire\b|\binternship\b|\bintern\b", re.I), "Stage"),
    (re.compile(r"\balternance\b|\balternant\b|\bapprentissage\b|\bapprentice\b", re.I), "Alternance"),
    (re.compile(r"\bv\.?i\.?e\.?\b|\bvolontariat\s+international\b", re.I), "V.I.E."),
    (re.compile(r"\bcdd\b|\bfixed.term\b|\bcontract\b|\btemporary\b", re.I), "CDD"),
    (re.compile(r"\bpart.time\b|\btemps\s+partiel\b", re.I), "Temps partiel"),
]

EXPERIENCE_MAP = {
    "no_experience":   "0 - 2 ans",
    "entry_level":     "0 - 2 ans",
    "mid_senior_level":"3 - 5 ans",
    "director":        "6 - 10 ans",
    "executive":       "11 ans et plus",
    "internship":      "0 - 2 ans",
    "associate":       "0 - 2 ans",
}


def _detect_contract(title: str, sr_type: str) -> str:
    for pat, ctype in _CONTRACT_PATTERNS:
        if pat.search(title):
            return ctype
    return CONTRACT_MAP.get(sr_type, "CDI")


def _html_to_text(html: str) -> str:
    if not html:
        return ""
    soup = BeautifulSoup(html, "html.parser")
    return soup.get_text(separator="\n").strip()


def _country_from_code(code: str) -> str:
    """Convertit le code ISO2 de SmartRecruiters en nom de pays français."""
    mapping = {
        "fr": "France", "gb": "Royaume-Uni", "de": "Allemagne", "es": "Espagne",
        "it": "Italie", "be": "Belgique", "lu": "Luxembourg", "nl": "Pays-Bas",
        "ch": "Suisse", "pl": "Pologne", "pt": "Portugal", "at": "Autriche",
        "se": "Suède", "dk": "Danemark", "no": "Norvège", "fi": "Finlande",
        "ie": "Irlande", "cz": "Tchéquie", "hu": "Hongrie", "ro": "Roumanie",
        "us": "États-Unis", "ca": "Canada", "au": "Australie", "sg": "Singapour",
        "hk": "Hong Kong", "cn": "Chine", "jp": "Japon", "in": "Inde",
        "za": "Afrique du Sud", "ma": "Maroc", "ng": "Nigeria", "ke": "Kenya",
        "eg": "Égypte", "ae": "Émirats arabes unis", "br": "Brésil",
        "mx": "Mexique", "ar": "Argentine", "cl": "Chili", "co": "Colombie",
    }
    return mapping.get((code or "").lower(), normalize_country(code or ""))


# ─── Base de données ──────────────────────────────────────────────────────────
def init_db(conn: sqlite3.Connection):
    conn.execute("""
    CREATE TABLE IF NOT EXISTS jobs (
        job_url        TEXT PRIMARY KEY,
        sr_id          TEXT UNIQUE,
        job_title      TEXT,
        contract_type  TEXT,
        publication_date TEXT,
        location       TEXT,
        city           TEXT,
        country        TEXT,
        region         TEXT,
        department     TEXT,
        job_family     TEXT,
        experience_level TEXT,
        education_level  TEXT,
        job_description  TEXT,
        company_name   TEXT DEFAULT 'Mazars',
        status         TEXT DEFAULT 'Live',
        is_valid       INTEGER DEFAULT 1,
        first_seen     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        last_updated   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )""")
    # Migration si colonne manquante
    existing = {r[1] for r in conn.execute("PRAGMA table_info(jobs)")}
    for col, dflt in [("is_valid", "1"), ("city", "''"), ("region", "''")]:
        if col not in existing:
            conn.execute(f"ALTER TABLE jobs ADD COLUMN {col} INTEGER DEFAULT {dflt}")
    conn.commit()


def upsert_job(conn: sqlite3.Connection, row: dict):
    conn.execute("""
    INSERT INTO jobs (
        job_url, sr_id, job_title, contract_type, publication_date,
        location, city, country, region, department, job_family,
        experience_level, education_level, job_description,
        company_name, status, is_valid, first_seen, last_updated
    ) VALUES (
        :job_url, :sr_id, :job_title, :contract_type, :publication_date,
        :location, :city, :country, :region, :department, :job_family,
        :experience_level, :education_level, :job_description,
        :company_name, :status, 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
    )
    ON CONFLICT(sr_id) DO UPDATE SET
        job_title        = excluded.job_title,
        contract_type    = excluded.contract_type,
        location         = excluded.location,
        city             = excluded.city,
        country          = excluded.country,
        region           = excluded.region,
        department       = excluded.department,
        job_family       = excluded.job_family,
        experience_level = excluded.experience_level,
        status           = excluded.status,
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
    expired = conn.execute(
        "SELECT sr_id FROM jobs WHERE is_valid=1"
    ).fetchall()
    count = 0
    for (sr_id,) in expired:
        if sr_id not in live_ids:
            conn.execute(
                "UPDATE jobs SET is_valid=0, status='Expired', last_updated=CURRENT_TIMESTAMP WHERE sr_id=?",
                (sr_id,)
            )
            count += 1
    if count:
        logger.info(f"  ⚠️  {count} offres marquées expirées")
    conn.commit()


def get_existing_sr_ids(conn: sqlite3.Connection) -> set:
    return {r[0] for r in conn.execute("SELECT sr_id FROM jobs WHERE sr_id IS NOT NULL")}


def get_sr_ids_without_description(conn: sqlite3.Connection) -> list:
    return [r[0] for r in conn.execute(
        "SELECT sr_id FROM jobs WHERE is_valid=1 AND (job_description IS NULL OR job_description='')"
    )]


# ─── API SmartRecruiters ──────────────────────────────────────────────────────
def fetch_listing_page(session: requests.Session, offset: int) -> dict:
    resp = session.get(
        f"{API_BASE}/postings",
        params={"limit": PAGE_SIZE, "offset": offset},
        headers=HEADERS,
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()


def fetch_detail(session: requests.Session, sr_id: str) -> Optional[dict]:
    try:
        resp = session.get(
            f"{API_BASE}/postings/{sr_id}",
            headers=HEADERS,
            timeout=30,
        )
        resp.raise_for_status()
        return resp.json()
    except Exception as exc:
        logger.warning(f"    ⚠️  Détail {sr_id}: {exc}")
        return None


def parse_listing_job(job: dict) -> dict:
    """Construit un dict partiel depuis la réponse listing."""
    sr_id    = job.get("id", "")
    title    = (job.get("name") or "").strip()
    released = (job.get("releasedDate") or "")[:10]
    loc      = job.get("location", {})
    city     = loc.get("city", "")
    country_code = loc.get("country", "")
    country  = _country_from_code(country_code)
    region   = loc.get("region", "")
    full_loc = loc.get("fullLocation", "") or f"{city}, {country}"
    sr_type  = (job.get("typeOfEmployment") or {}).get("id", "permanent")
    contract = _detect_contract(title, sr_type)
    exp_id   = (job.get("experienceLevel") or {}).get("id", "")
    exp      = EXPERIENCE_MAP.get(exp_id, "")
    dept     = (job.get("department") or {}).get("label", "")
    func     = (job.get("function") or {}).get("label", "")
    job_family = classify_job_family(f"{title} {dept} {func}")
    url      = f"https://jobs.smartrecruiters.com/{COMPANY_SLUG}/{sr_id}"

    return {
        "job_url":          url,
        "sr_id":            sr_id,
        "job_title":        title,
        "contract_type":    contract,
        "publication_date": released,
        "location":         full_loc,
        "city":             city,
        "country":          country,
        "region":           region,
        "department":       dept,
        "job_family":       job_family,
        "experience_level": exp,
        "education_level":  "",
        "job_description":  None,
        "company_name":     "Mazars",
        "status":           "Live",
    }


def enrich_with_detail(job_row: dict, detail: dict) -> dict:
    """Enrichit un job_row avec les données du détail."""
    sections = detail.get("jobAd", {}).get("sections", {})
    parts = []
    for key in ("companyDescription", "jobDescription", "qualifications", "additionalInformation"):
        text = _html_to_text((sections.get(key) or {}).get("text", ""))
        if text:
            parts.append(text)
    description = "\n\n".join(parts)
    if description:
        job_row["job_description"] = description
        edu = extract_education_level(description)
        if edu:
            job_row["education_level"] = edu
        # Affiner expérience depuis la description
        exp = extract_experience_level(description)
        if exp and not job_row.get("experience_level"):
            job_row["experience_level"] = exp

    # URL candidature (plus précise)
    posting_url = detail.get("postingUrl", "")
    if posting_url:
        job_row["job_url"] = posting_url

    return job_row


# ─── Main ─────────────────────────────────────────────────────────────────────
def main():
    t0 = time.time()
    logger.info("=" * 60)
    logger.info("Forvis Mazars Scraper — démarrage")

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    init_db(conn)

    existing_ids = get_existing_sr_ids(conn)
    session = requests.Session()

    # ── Phase 1 : listing complet ──────────────────────────────────────────
    logger.info("📋 Phase 1 — listing SmartRecruiters...")
    all_jobs: list[dict] = []
    offset = 0
    while True:
        data = fetch_listing_page(session, offset)
        jobs = data.get("content", [])
        if not jobs:
            break
        all_jobs.extend(jobs)
        total = data.get("totalFound", 0)
        logger.info(f"   offset={offset} → {len(jobs)} offres (total: {total})")
        if offset + PAGE_SIZE >= total:
            break
        offset += PAGE_SIZE
        time.sleep(REQUEST_PAUSE)

    logger.info(f"   → {len(all_jobs)} offres récupérées")

    live_ids = {j.get("id") for j in all_jobs}
    new_count = 0
    rows = {}

    for job in all_jobs:
        row = parse_listing_job(job)
        sr_id = row["sr_id"]
        rows[sr_id] = row
        if sr_id not in existing_ids:
            new_count += 1

    logger.info(f"   → {new_count} nouvelles offres, {len(all_jobs)-new_count} déjà connues")

    # ── Phase 2 : détails pour nouvelles offres ──────────────────────────
    logger.info("📝 Phase 2 — enrichissement descriptions...")
    # Nouvelles + sans description
    ids_without_desc = set(get_sr_ids_without_description(conn))
    ids_to_enrich = (live_ids - existing_ids) | (live_ids & ids_without_desc)
    ids_to_enrich = list(ids_to_enrich)[:DETAIL_CAP]

    enriched = 0
    for i, sr_id in enumerate(ids_to_enrich):
        if sr_id not in rows:
            continue
        detail = fetch_detail(session, sr_id)
        if detail:
            rows[sr_id] = enrich_with_detail(rows[sr_id], detail)
            enriched += 1
        if (i + 1) % 20 == 0:
            logger.info(f"   {i+1}/{len(ids_to_enrich)} descriptions enrichies")
        time.sleep(REQUEST_PAUSE)

    logger.info(f"   → {enriched} descriptions enrichies")

    # ── Phase 3 : UPSERT en base ─────────────────────────────────────────
    for row in rows.values():
        upsert_job(conn, row)
    conn.commit()

    # ── Phase 4 : marquer les expirées ───────────────────────────────────
    mark_expired(conn, live_ids)

    total_live = conn.execute("SELECT COUNT(*) FROM jobs WHERE is_valid=1").fetchone()[0]
    elapsed = time.time() - t0

    logger.info("=" * 60)
    logger.info(f"✅ Mazars — {total_live} offres Live en base")
    logger.info(f"   Nouvelles  : {new_count}")
    logger.info(f"   Enrichies  : {enriched}")
    logger.info(f"   Durée      : {elapsed:.1f}s")
    logger.info(f"   Base       : {DB_PATH}")
    logger.info("=" * 60)
    conn.close()


if __name__ == "__main__":
    main()
