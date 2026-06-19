#!/usr/bin/env python3
"""
Stripe — Job Scraper
=====================
Source : Greenhouse API publique (board "stripe")
  Endpoint : https://boards-api.greenhouse.io/v1/boards/stripe/jobs?content=true
  → Retourne TOUS les jobs en une seule requête, description HTML incluse

Delta scraping : gh_id (Greenhouse job ID) comme clé unique
"""
import json
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
              logging.FileHandler("stripe_scraper.log", encoding="utf-8")],
)
logger = logging.getLogger(__name__)

# ─── Config ──────────────────────────────────────────────────────────────────
BOARD_SLUG   = "stripe"
COMPANY_NAME = "Stripe"
API_URL      = f"https://boards-api.greenhouse.io/v1/boards/{BOARD_SLUG}/jobs?content=true"
DB_PATH      = Path(__file__).parent / "stripe_jobs.db"
REQUEST_PAUSE = 0.2

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
    "Accept": "application/json",
}

# Greenhouse employment_type → Taleos
_TYPE_PATTERNS = [
    (re.compile(r"\binternship\b|\bintern\b|\bstage\b", re.I), "Stage"),
    (re.compile(r"\bapprentice\b|\balternance\b", re.I), "Alternance"),
    (re.compile(r"\bcontract\b|\btemporary\b|\bcdd\b|\bfixed.term\b", re.I), "CDD"),
    (re.compile(r"\bpart.time\b", re.I), "Temps partiel"),
]


def _detect_contract(title: str, emp_type: str = "") -> str:
    combined = f"{title} {emp_type}"
    for pat, ctype in _TYPE_PATTERNS:
        if pat.search(combined):
            return ctype
    return "CDI"


def _html_to_text(html: str) -> str:
    if not html:
        return ""
    return BeautifulSoup(html, "html.parser").get_text(separator="\n").strip()


def _parse_location(loc_name: str) -> tuple[str, str]:
    """Retourne (city, country) depuis 'City, Country'."""
    if not loc_name or loc_name.strip().upper() == 'N/A':
        return "Remote", "Remote"
    parts = [p.strip() for p in loc_name.split(",")]
    if len(parts) >= 2:
        return parts[0], normalize_country(parts[-1])
    return parts[0], normalize_country(parts[0])


# ─── Base de données ──────────────────────────────────────────────────────────
def init_db(conn: sqlite3.Connection):
    conn.execute("""
    CREATE TABLE IF NOT EXISTS jobs (
        job_url          TEXT PRIMARY KEY,
        gh_id            TEXT UNIQUE,
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
        company_name     TEXT DEFAULT 'Stripe',
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
        job_url, gh_id, job_title, contract_type, publication_date,
        location, city, country, region, department, job_family,
        experience_level, education_level, job_description,
        company_name, status, is_valid, first_seen, last_updated
    ) VALUES (
        :job_url, :gh_id, :job_title, :contract_type, :publication_date,
        :location, :city, :country, :region, :department, :job_family,
        :experience_level, :education_level, :job_description,
        :company_name, :status, 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
    )
    ON CONFLICT(gh_id) DO UPDATE SET
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
    count = 0
    for (gid,) in conn.execute("SELECT gh_id FROM jobs WHERE is_valid=1"):
        if gid not in live_ids:
            conn.execute(
                "UPDATE jobs SET is_valid=0, status='Expired', last_updated=CURRENT_TIMESTAMP WHERE gh_id=?",
                (gid,)
            )
            count += 1
    if count:
        logger.info(f"  ⚠️  {count} offres marquées expirées")
    conn.commit()


def get_existing_ids(conn: sqlite3.Connection) -> set:
    return {r[0] for r in conn.execute("SELECT gh_id FROM jobs WHERE gh_id IS NOT NULL")}


# ─── API Greenhouse ───────────────────────────────────────────────────────────
def fetch_all_jobs(session: requests.Session) -> list:
    """Greenhouse retourne TOUS les jobs en une seule requête."""
    resp = session.get(API_URL, headers=HEADERS, timeout=60)
    resp.raise_for_status()
    return resp.json().get("jobs", [])


def parse_job(job: dict) -> dict:
    gh_id    = str(job.get("id", ""))
    title    = (job.get("title") or "").strip()
    url      = job.get("absolute_url", "")
    loc_name = (job.get("location") or {}).get("name", "") or ""
    if loc_name.strip().upper() == "N/A":
        loc_name = "Remote"
    city, country = _parse_location(loc_name)
    dept     = ", ".join(d.get("name", "") for d in (job.get("departments") or []))
    updated  = (job.get("updated_at") or "")[:10]
    content  = _html_to_text(job.get("content", "") or "")
    contract = _detect_contract(title)
    job_family = classify_job_family(f"{title} {dept}")
    exp      = extract_experience_level(content) if content else ""
    edu      = extract_education_level(content) if content else ""

    return {
        "job_url":          url,
        "gh_id":            gh_id,
        "job_title":        title,
        "contract_type":    contract,
        "publication_date": updated,
        "location":         loc_name,
        "city":             city,
        "country":          country,
        "region":           "",
        "department":       dept,
        "job_family":       job_family,
        "experience_level": exp,
        "education_level":  edu,
        "job_description":  content,
        "company_name":     COMPANY_NAME,
        "status":           "Live",
    }


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

    logger.info("📋 Phase 1 — listing Greenhouse...")
    all_jobs = fetch_all_jobs(session)
    logger.info(f"   → {len(all_jobs)} offres récupérées")

    live_ids = {str(j.get("id", "")) for j in all_jobs}
    new_count = 0
    rows = {}

    for job in all_jobs:
        row = parse_job(job)
        gid = row["gh_id"]
        rows[gid] = row
        if gid not in existing_ids:
            new_count += 1

    logger.info(f"   → {new_count} nouvelles, {len(all_jobs)-new_count} déjà connues")

    for row in rows.values():
        upsert_job(conn, row)
    conn.commit()

    mark_expired(conn, live_ids)

    total_live = conn.execute("SELECT COUNT(*) FROM jobs WHERE is_valid=1").fetchone()[0]
    elapsed = time.time() - t0

    logger.info("=" * 60)
    logger.info(f"✅ {COMPANY_NAME} — {total_live} offres Live en base")
    logger.info(f"   Nouvelles : {new_count}")
    logger.info(f"   Durée     : {elapsed:.1f}s")
    logger.info(f"   Base      : {DB_PATH}")
    logger.info("=" * 60)
    conn.close()


if __name__ == "__main__":
    main()
