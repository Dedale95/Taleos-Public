#!/usr/bin/env python3
"""
Moelis & Company — Job Scraper
================================
Source : Workday API (wd1)
  Tenant : moelis
  Boards : Experienced-Hires, University-Hires
  API    : POST https://moelis.wd1.myworkdayjobs.com/wday/cxs/moelis/{board}/jobs

Delta scraping : wd_id (externalPath) comme clé unique
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
              logging.FileHandler("moelis_scraper.log", encoding="utf-8")],
)
logger = logging.getLogger(__name__)

# ─── Config ──────────────────────────────────────────────────────────────────
WD_HOST      = "https://moelis.wd1.myworkdayjobs.com"
WD_COMPANY   = "moelis"
BOARDS       = ["Experienced-Hires", "University-Hires"]
DB_PATH      = Path(__file__).parent / "moelis_jobs.db"
PAGE_SIZE    = 20   # Moelis Workday: max 20/page
REQUEST_PAUSE = 0.4
COMPANY_NAME = "Moelis & Company"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "application/json,text/html,*/*",
    "Content-Type": "application/json",
}

_CONTRACT_PATTERNS = [
    (re.compile(r"\binternship\b|\bintern\b|\bstage\b|\bstagiaire\b", re.I), "Stage"),
    (re.compile(r"\balternance\b|\bapprentice\b", re.I), "Alternance"),
    (re.compile(r"\bv\.?i\.?e\.?\b|\bvolontariat\s+international\b", re.I), "V.I.E."),
    (re.compile(r"\bfixed.term\b|\btemporary\b|\bcdd\b|\bcontract\b", re.I), "CDD"),
]

LEVEL_MAP = {
    "Senior": "6 - 10 ans", "Director": "11 ans et plus", "VP": "6 - 10 ans",
    "Associate": "3 - 5 ans", "Analyst": "0 - 2 ans", "Intern": "0 - 2 ans",
    "Managing Director": "11 ans et plus", "MD": "11 ans et plus",
    "Principal": "11 ans et plus", "Partner": "11 ans et plus",
}


def _detect_contract(title: str) -> str:
    for pat, ctype in _CONTRACT_PATTERNS:
        if pat.search(title or ""):
            return ctype
    return "CDI"


def _detect_level(title: str) -> str:
    for keyword, level in LEVEL_MAP.items():
        if re.search(r'\b' + re.escape(keyword) + r'\b', title or "", re.I):
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
        company_name     TEXT DEFAULT 'Moelis & Company',
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
    for (wid,) in conn.execute("SELECT wd_id FROM jobs WHERE is_valid=1"):
        if wid not in live_ids:
            conn.execute(
                "UPDATE jobs SET is_valid=0, status='Expired', last_updated=CURRENT_TIMESTAMP WHERE wd_id=?",
                (wid,)
            )
            count += 1
    if count:
        logger.info(f"  ⚠️  {count} offres marquées expirées")
    conn.commit()


def get_existing_ids(conn: sqlite3.Connection) -> set:
    return {r[0] for r in conn.execute("SELECT wd_id FROM jobs WHERE wd_id IS NOT NULL")}


def get_ids_without_description(conn: sqlite3.Connection) -> list:
    return [r[0] for r in conn.execute(
        "SELECT wd_id FROM jobs WHERE is_valid=1 AND (job_description IS NULL OR job_description='')"
    )]


# ─── API Workday ──────────────────────────────────────────────────────────────
def fetch_board_jobs(session: requests.Session, board: str) -> list:
    api_url = f"{WD_HOST}/wday/cxs/{WD_COMPANY}/{board}/jobs"
    all_jobs = []
    offset = 0
    total = None
    while True:
        payload = {"appliedFacets": {}, "limit": PAGE_SIZE, "offset": offset, "searchText": ""}
        resp = session.post(api_url, json=payload, headers=HEADERS, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        batch = data.get("jobPostings") or []
        all_jobs.extend(batch)
        if total is None:
            total = data.get("total", 0)
        logger.info(f"   [{board}] offset={offset} → {len(batch)} offres (total: {total})")
        if not batch or offset + PAGE_SIZE >= total:
            break
        offset += PAGE_SIZE
        time.sleep(REQUEST_PAUSE)
    return all_jobs


def fetch_detail(session: requests.Session, board: str, external_path: str) -> Optional[dict]:
    try:
        # external_path is already like "/job/City/Title_REQxxx" — no need to add "job"
        api_url = f"{WD_HOST}/wday/cxs/{WD_COMPANY}/{board}{external_path}"
        resp = session.get(api_url, headers=HEADERS, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        return data.get("jobPostingInfo") or data
    except Exception as exc:
        logger.warning(f"    ⚠️  Détail {external_path}: {exc}")
        return None


def parse_job(job: dict, board: str) -> dict:
    title        = (job.get("title") or "").strip()
    ext_path     = job.get("externalPath", "")
    wd_id        = ext_path  # chemin unique par job
    location_txt = job.get("locationsText", "")
    posted       = (job.get("postedOn") or "")[:10]
    # Parse location
    parts = [p.strip() for p in location_txt.split(",")]
    city    = parts[0] if parts else ""
    country = normalize_country(parts[-1]) if len(parts) > 1 else ""
    contract  = _detect_contract(title)
    exp_level = _detect_level(title)
    job_family = classify_job_family(title)
    url = f"{WD_HOST}/en-US/{board}{ext_path}"
    return {
        "job_url": url, "wd_id": wd_id, "job_title": title,
        "contract_type": contract, "publication_date": posted,
        "location": location_txt, "city": city, "country": country, "region": "",
        "department": board.replace("-", " "), "job_family": job_family,
        "experience_level": exp_level, "education_level": "",
        "job_description": None, "company_name": COMPANY_NAME, "status": "Live",
    }


def enrich_with_detail(row: dict, detail: dict) -> dict:
    desc_html = detail.get("jobDescription") or detail.get("externalDescriptionStr") or ""
    description = _html_to_text(desc_html)
    if description:
        row["job_description"] = description
        edu = extract_education_level(description)
        if edu: row["education_level"] = edu
        if not row.get("experience_level"):
            exp = extract_experience_level(description)
            if exp: row["experience_level"] = exp
    # Affiner localisation depuis le détail
    for loc_key in ("primaryLocations", "locations"):
        locs = detail.get(loc_key) or []
        if locs:
            first = locs[0]
            city = first.get("city") or first.get("cityRegionState") or row["city"]
            iso  = first.get("countryIso2Code") or ""
            country_map = {
                "US": "États-Unis", "GB": "Royaume-Uni", "FR": "France",
                "DE": "Allemagne",  "IT": "Italie",      "ES": "Espagne",
                "CH": "Suisse",     "HK": "Hong Kong",   "SG": "Singapour",
                "AU": "Australie",  "CA": "Canada",       "JP": "Japon",
                "AE": "Émirats arabes unis",
            }
            row["city"] = city
            if iso: row["country"] = country_map.get(iso.upper(), normalize_country(iso))
            break
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

    all_jobs_by_board = {}
    all_wd_ids = set()

    for board in BOARDS:
        logger.info(f"📋 Listing board [{board}]...")
        jobs = fetch_board_jobs(session, board)
        for job in jobs:
            ext_path = job.get("externalPath", "")
            if ext_path:
                all_jobs_by_board[ext_path] = (job, board)
                all_wd_ids.add(ext_path)

    logger.info(f"   → {len(all_wd_ids)} offres récupérées total")

    rows = {}
    new_count = 0
    for wd_id, (job, board) in all_jobs_by_board.items():
        row = parse_job(job, board)
        rows[wd_id] = (row, board)
        if wd_id not in existing_ids:
            new_count += 1

    logger.info(f"   → {new_count} nouvelles, {len(all_wd_ids)-new_count} déjà connues")

    logger.info("📝 Phase 2 — enrichissement descriptions...")
    ids_without = set(get_ids_without_description(conn))
    ids_to_enrich = list((all_wd_ids - existing_ids) | (all_wd_ids & ids_without))
    enriched = 0
    for wd_id in ids_to_enrich:
        if wd_id not in rows:
            continue
        row, board = rows[wd_id]
        detail = fetch_detail(session, board, wd_id)
        if detail:
            rows[wd_id] = (enrich_with_detail(row, detail), board)
            enriched += 1
        time.sleep(REQUEST_PAUSE)
    logger.info(f"   → {enriched} descriptions enrichies")

    for wd_id, (row, _) in rows.items():
        upsert_job(conn, row)
    conn.commit()
    mark_expired(conn, all_wd_ids)

    total_live = conn.execute("SELECT COUNT(*) FROM jobs WHERE is_valid=1").fetchone()[0]
    elapsed = time.time() - t0
    logger.info("=" * 60)
    logger.info(f"✅ {COMPANY_NAME} — {total_live} offres Live en base")
    logger.info(f"   Nouvelles : {new_count}  Enrichies : {enriched}  Durée : {elapsed:.1f}s")
    logger.info("=" * 60)
    conn.close()


if __name__ == "__main__":
    main()
