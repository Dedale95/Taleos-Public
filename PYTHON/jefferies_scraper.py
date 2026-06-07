#!/usr/bin/env python3
"""
Jefferies — Job Scraper
========================
Source : Oracle Cloud HCM (portal carrières Jefferies)
  Host       : hdid.fa.us2.oraclecloud.com
  Site       : CX_1
  API listing : /hcmRestApi/resources/latest/recruitingCEJobRequisitions
  API détail  : /hcmRestApi/resources/latest/recruitingCEJobRequisitionDetails

Delta scraping : oracle_id (Id) comme clé unique (~192 offres)
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
    handlers=[logging.StreamHandler(sys.stdout),
              logging.FileHandler("jefferies_scraper.log", encoding="utf-8")],
)
logger = logging.getLogger(__name__)

# ─── Config ──────────────────────────────────────────────────────────────────
ORACLE_HOST   = "https://hdid.fa.us2.oraclecloud.com"
SITE_NUMBER   = "CX_1"
API_LISTING   = f"{ORACLE_HOST}/hcmRestApi/resources/latest/recruitingCEJobRequisitions"
API_DETAIL    = f"{ORACLE_HOST}/hcmRestApi/resources/latest/recruitingCEJobRequisitionDetails"
DB_PATH       = Path(__file__).parent / "jefferies_jobs.db"
PAGE_SIZE     = 100
REQUEST_PAUSE = 0.4
COMPANY_NAME  = "Jefferies"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
    "Accept": "application/json",
}

_CONTRACT_PATTERNS = [
    (re.compile(r"\binternship\b|\bintern\b|\bstage\b|\bstagiaire\b", re.I), "Stage"),
    (re.compile(r"\balternance\b|\bapprentice\b", re.I), "Alternance"),
    (re.compile(r"\bv\.?i\.?e\.?\b|\bvolontariat\s+international\b", re.I), "V.I.E."),
    (re.compile(r"\bfixed.term\b|\btemporary\b|\bcdd\b|\bcontract\b", re.I), "CDD"),
    (re.compile(r"\bpart.time\b", re.I), "Temps partiel"),
]

LEVEL_MAP = {
    "Senior":    "6 - 10 ans", "Mid":       "3 - 5 ans",  "Junior":    "0 - 2 ans",
    "Director":  "11 ans et plus", "VP":    "6 - 10 ans",  "Associate": "3 - 5 ans",
    "Analyst":   "0 - 2 ans",  "Intern":    "0 - 2 ans",   "Manager":   "6 - 10 ans",
    "Principal": "11 ans et plus", "MD":   "11 ans et plus",
}

ISO2_COUNTRY = {
    "US": "États-Unis", "GB": "Royaume-Uni", "FR": "France", "DE": "Allemagne",
    "IT": "Italie",     "ES": "Espagne",     "CH": "Suisse", "BE": "Belgique",
    "NL": "Pays-Bas",   "LU": "Luxembourg",  "SE": "Suède",  "DK": "Danemark",
    "NO": "Norvège",    "AT": "Autriche",    "PL": "Pologne","AE": "Émirats arabes unis",
    "SG": "Singapour",  "HK": "Hong Kong",   "JP": "Japon",  "AU": "Australie",
    "CA": "Canada",     "BR": "Brésil",      "IN": "Inde",   "CN": "Chine",
}


def _detect_contract(title: str, oracle_contract: Optional[str] = None) -> str:
    for pat, ctype in _CONTRACT_PATTERNS:
        if pat.search(title or ""):
            return ctype
    c = (oracle_contract or "").upper()
    if "INTERN" in c: return "Stage"
    if "FIXED" in c:  return "CDD"
    return "CDI"


def _detect_level(title: str, job_level: Optional[str] = None) -> str:
    combined = f"{title} {job_level or ''}"
    for keyword, level in LEVEL_MAP.items():
        if re.search(r'\b' + keyword + r'\b', combined, re.I):
            return level
    return ""


def _html_to_text(html: str) -> str:
    if not html:
        return ""
    return BeautifulSoup(html, "html.parser").get_text(separator="\n").strip()


# ─── Base de données ──────────────────────────────────────────────────────────
def init_db(conn: sqlite3.Connection):
    conn.execute("""
    CREATE TABLE IF NOT EXISTS jobs (
        job_url          TEXT PRIMARY KEY,
        oracle_id        TEXT UNIQUE,
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
        company_name     TEXT DEFAULT 'Jefferies',
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
        job_url, oracle_id, job_title, contract_type, publication_date,
        location, city, country, region, department, job_family,
        experience_level, education_level, job_description,
        company_name, status, is_valid, first_seen, last_updated
    ) VALUES (
        :job_url, :oracle_id, :job_title, :contract_type, :publication_date,
        :location, :city, :country, :region, :department, :job_family,
        :experience_level, :education_level, :job_description,
        :company_name, :status, 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
    )
    ON CONFLICT(oracle_id) DO UPDATE SET
        job_title        = excluded.job_title,
        contract_type    = excluded.contract_type,
        location         = excluded.location,
        city             = excluded.city,
        country          = excluded.country,
        department       = excluded.department,
        job_family       = excluded.job_family,
        experience_level = excluded.experience_level,
        status           = excluded.status,
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
    for (oid,) in conn.execute("SELECT oracle_id FROM jobs WHERE is_valid=1"):
        if oid not in live_ids:
            conn.execute(
                "UPDATE jobs SET is_valid=0, status='Expired', last_updated=CURRENT_TIMESTAMP WHERE oracle_id=?",
                (oid,)
            )
            count += 1
    if count:
        logger.info(f"  ⚠️  {count} offres marquées expirées")
    conn.commit()


def get_existing_ids(conn: sqlite3.Connection) -> set:
    return {r[0] for r in conn.execute("SELECT oracle_id FROM jobs WHERE oracle_id IS NOT NULL")}


def get_ids_without_description(conn: sqlite3.Connection) -> list:
    return [r[0] for r in conn.execute(
        "SELECT oracle_id FROM jobs WHERE is_valid=1 AND (job_description IS NULL OR job_description='')"
    )]


# ─── API Oracle ───────────────────────────────────────────────────────────────
def fetch_all_listings(session: requests.Session) -> list:
    all_reqs = []
    offset = 0
    while True:
        params = {
            "onlyData": "true",
            "expand": "requisitionList",
            "finder": f"findReqs;siteNumber={SITE_NUMBER},limit={PAGE_SIZE},offset={offset}",
        }
        resp = session.get(API_LISTING, params=params, headers=HEADERS, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        items = data.get("items", [])
        if not items:
            break
        reqs = items[0].get("requisitionList", [])
        all_reqs.extend(reqs)
        total = items[0].get("TotalJobsCount", 0)
        logger.info(f"   offset={offset} → {len(reqs)} offres (total: {total})")
        if offset + PAGE_SIZE >= total or not reqs:
            break
        offset += PAGE_SIZE
        time.sleep(REQUEST_PAUSE)
    return all_reqs


def fetch_detail(session: requests.Session, oracle_id: str) -> Optional[dict]:
    try:
        params = {
            "onlyData": "true",
            "finder": f"ById;Id={oracle_id},siteNumber={SITE_NUMBER}",
        }
        resp = session.get(API_DETAIL, params=params, headers=HEADERS, timeout=30)
        resp.raise_for_status()
        items = resp.json().get("items", [])
        return items[0] if items else None
    except Exception as exc:
        logger.warning(f"    ⚠️  Détail {oracle_id}: {exc}")
        return None


def parse_listing_req(req: dict) -> dict:
    oracle_id   = str(req.get("Id", ""))
    title       = (req.get("Title") or "").strip()
    posted      = (req.get("PostedDate") or "")[:10]
    location    = req.get("PrimaryLocation", "")
    country_iso = req.get("PrimaryLocationCountry", "")
    country     = ISO2_COUNTRY.get(country_iso.upper(), normalize_country(country_iso))
    city        = location.split(",")[0].strip() if "," in location else location
    contract    = _detect_contract(title, req.get("ContractType") or req.get("JobType"))
    job_level   = req.get("ManagerLevel") or req.get("JobGrade") or ""
    exp_level   = _detect_level(title, job_level)
    job_func    = req.get("JobFunction") or req.get("JobFamily") or ""
    dept        = req.get("Department") or req.get("BusinessUnit") or ""
    job_family  = classify_job_family(f"{title} {job_func} {dept}")
    url         = (f"{ORACLE_HOST}/hcmUI/CandidateExperience/en/sites/{SITE_NUMBER}"
                   f"/job/{oracle_id}")
    return {
        "job_url": url, "oracle_id": oracle_id, "job_title": title,
        "contract_type": contract, "publication_date": posted,
        "location": location, "city": city, "country": country, "region": "",
        "department": dept, "job_family": job_family,
        "experience_level": exp_level, "education_level": "",
        "job_description": None, "company_name": COMPANY_NAME, "status": "Live",
    }


def enrich_with_detail(row: dict, detail: dict) -> dict:
    parts = [_html_to_text(detail.get(k, "") or "")
             for k in ("ExternalDescriptionStr", "ExternalResponsibilitiesStr", "ExternalQualificationsStr")]
    description = "\n\n".join(p for p in parts if p)
    if description:
        row["job_description"] = description
        edu = extract_education_level(description)
        if edu: row["education_level"] = edu
        if not row.get("experience_level"):
            exp = extract_experience_level(description)
            if exp: row["experience_level"] = exp
    if not row.get("contract_type") or row["contract_type"] == "CDI":
        dc = detail.get("ContractType") or detail.get("RequisitionType") or ""
        row["contract_type"] = _detect_contract(row["job_title"], dc)
    return row


# ─── Main ─────────────────────────────────────────────────────────────────────
def main():
    t0 = time.time()
    logger.info("=" * 60)
    logger.info(f"{COMPANY_NAME} Scraper — démarrage")

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    init_db(conn)
    existing_ids = get_existing_ids(conn)
    session = requests.Session()

    logger.info("📋 Phase 1 — listing Oracle Cloud HCM...")
    all_reqs = fetch_all_listings(session)
    logger.info(f"   → {len(all_reqs)} offres récupérées")

    live_ids = {str(r.get("Id", "")) for r in all_reqs}
    rows = {}
    new_count = 0

    for req in all_reqs:
        row = parse_listing_req(req)
        oid = row["oracle_id"]
        rows[oid] = row
        if oid not in existing_ids:
            new_count += 1

    logger.info(f"   → {new_count} nouvelles, {len(all_reqs)-new_count} déjà connues")

    logger.info("📝 Phase 2 — enrichissement descriptions...")
    ids_without = set(get_ids_without_description(conn))
    ids_to_enrich = list((live_ids - existing_ids) | (live_ids & ids_without))
    enriched = 0
    for oid in ids_to_enrich:
        if oid not in rows:
            continue
        detail = fetch_detail(session, oid)
        if detail:
            rows[oid] = enrich_with_detail(rows[oid], detail)
            enriched += 1
        time.sleep(REQUEST_PAUSE)
    logger.info(f"   → {enriched} descriptions enrichies")

    for row in rows.values():
        upsert_job(conn, row)
    conn.commit()
    mark_expired(conn, live_ids)

    total_live = conn.execute("SELECT COUNT(*) FROM jobs WHERE is_valid=1").fetchone()[0]
    elapsed = time.time() - t0
    logger.info("=" * 60)
    logger.info(f"✅ {COMPANY_NAME} — {total_live} offres Live en base")
    logger.info(f"   Nouvelles : {new_count}  Enrichies : {enriched}  Durée : {elapsed:.1f}s")
    logger.info("=" * 60)
    conn.close()


if __name__ == "__main__":
    main()
