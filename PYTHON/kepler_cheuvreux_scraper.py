#!/usr/bin/env python3
"""
Kepler Cheuvreux — Scraper
===========================
Source : https://keplercheuvreux.teamtailor.com/jobs.json  (JSON Feed v1.1)
Méthode : GET unique — toutes les offres en un seul appel (pas de pagination)

Champs extraits :
  job_url, job_id, job_title, contract_type, publication_date,
  location, city, country, region,
  job_family, experience_level, education_level,
  job_description, company_name, status
"""
from __future__ import annotations

import logging
import re
import sqlite3
import time
from datetime import datetime
from html.parser import HTMLParser
from pathlib import Path

import requests

try:
    from job_family_classifier import classify_job_family
    from experience_extractor import extract_experience_level
    from education_extractor import extract_education_level
except ImportError:
    import sys
    sys.path.append(str(Path(__file__).parent))
    from job_family_classifier import classify_job_family
    from experience_extractor import extract_experience_level
    from education_extractor import extract_education_level

# ─────────────────────────── Logging ────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(Path(__file__).parent / "kepler_cheuvreux_scraper.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger(__name__)

# ─────────────────────────── Config ─────────────────────────────
DB_PATH      = Path(__file__).parent / "kepler_cheuvreux_jobs.db"
FEED_URL     = "https://keplercheuvreux.teamtailor.com/jobs.json"
COMPANY_NAME = "Kepler Cheuvreux"
REQUEST_TIMEOUT = 30

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, */*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9,fr;q=0.8",
}

# ─────────────── Mapping city/locality → (pays, région) ──────────────────
CITY_COUNTRY_MAP: dict[str, tuple[str, str]] = {
    "paris":         ("France",          "Europe"),
    "biot":          ("France",          "Europe"),
    "nyon":          ("Suisse",          "Europe"),
    "zurich":        ("Suisse",          "Europe"),
    "geneva":        ("Suisse",          "Europe"),
    "genève":        ("Suisse",          "Europe"),
    "london":        ("Royaume-Uni",     "Europe"),
    "frankfurt":     ("Allemagne",       "Europe"),
    "madrid":        ("Espagne",         "Europe"),
    "amsterdam":     ("Pays-Bas",        "Europe"),
    "milan":         ("Italie",          "Europe"),
    "stockholm":     ("Suède",           "Europe"),
    "oslo":          ("Norvège",         "Europe"),
    "copenhagen":    ("Danemark",        "Europe"),
    "brussels":      ("Belgique",        "Europe"),
    "bruxelles":     ("Belgique",        "Europe"),
    "new york":      ("États-Unis",      "Amérique du Nord"),
    "san francisco": ("États-Unis",      "Amérique du Nord"),
    "boston":        ("États-Unis",      "Amérique du Nord"),
    "chicago":       ("États-Unis",      "Amérique du Nord"),
    "toronto":       ("Canada",          "Amérique du Nord"),
    "hong kong":     ("Hong Kong",       "Asie-Pacifique"),
    "singapore":     ("Singapour",       "Asie-Pacifique"),
    "tokyo":         ("Japon",           "Asie-Pacifique"),
    "dubai":         ("Émirats arabes unis", "Moyen-Orient / Afrique"),
}

# ─────────────── Patterns contrat depuis le titre ─────────────────────────
CONTRACT_PATTERNS = [
    (re.compile(r"\b(intern(ship)?|stage)\b", re.I),         "Stage"),
    (re.compile(r"\b(alternance|apprenti|alternant)\b", re.I), "Alternance"),
    (re.compile(r"\b(vie|volontariat)\b", re.I),             "VIE"),
    (re.compile(r"\b(cdd|fixed.term|temporary)\b", re.I),    "CDD"),
    (re.compile(r"\b(cdi|permanent|full.time)\b", re.I),     "CDI"),
]


def _html_to_text(html: str) -> str:
    """Convertit HTML en texte brut simple."""
    class _P(HTMLParser):
        def __init__(self):
            super().__init__()
            self.parts: list[str] = []
        def handle_data(self, data: str):
            self.parts.append(data)
    p = _P()
    p.feed(html or "")
    return " ".join(p.parts).strip()


def _extract_locations(job_posting: dict) -> list[str]:
    """Extrait les villes depuis _jobposting.jobLocation."""
    locs = job_posting.get("jobLocation") or {}
    if isinstance(locs, dict):
        locs = [locs]
    cities = []
    for loc in locs:
        addr = loc.get("address") or {}
        city = addr.get("addressLocality") or addr.get("addressRegion") or ""
        if city:
            cities.append(city.strip())
    return cities


def _resolve_country_region(cities: list[str]) -> tuple[str, str, str]:
    """Retourne (location_str, country, region) depuis une liste de villes."""
    location = ", ".join(cities) if cities else "Non spécifié"
    for city in cities:
        key = city.lower()
        if key in CITY_COUNTRY_MAP:
            return location, CITY_COUNTRY_MAP[key][0], CITY_COUNTRY_MAP[key][1]
    # Fallback : cherche une sous-chaîne
    for city in cities:
        for k, (country, region) in CITY_COUNTRY_MAP.items():
            if k in city.lower():
                return location, country, region
    return location, "Non spécifié", "Non spécifié"


def _guess_contract(title: str) -> str:
    for pat, ctype in CONTRACT_PATTERNS:
        if pat.search(title):
            return ctype
    return "CDI"


def _extract_job_id(url: str) -> str:
    """Extrait l'ID numérique du slug Teamtailor (ex: jobs/7837681-us-equity...)."""
    m = re.search(r"/jobs/(\d+)", url)
    return m.group(1) if m else ""


# ══════════════════════════════════════════════════════════════════
# Database
# ══════════════════════════════════════════════════════════════════

def init_db(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS jobs (
            job_url          TEXT PRIMARY KEY,
            job_id           TEXT,
            job_title        TEXT,
            contract_type    TEXT,
            publication_date TEXT,
            location         TEXT,
            city             TEXT,
            country          TEXT,
            region           TEXT,
            job_family       TEXT,
            experience_level TEXT,
            education_level  TEXT,
            job_description  TEXT,
            company_name     TEXT DEFAULT 'Kepler Cheuvreux',
            status           TEXT DEFAULT 'Live',
            is_valid         INTEGER DEFAULT 1,
            scraped_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            first_seen       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            last_updated     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.commit()
    return conn


def upsert_job(conn: sqlite3.Connection, job: dict) -> None:
    now = datetime.utcnow().isoformat()
    conn.execute("""
        INSERT INTO jobs
            (job_url, job_id, job_title, contract_type, publication_date,
             location, city, country, region,
             job_family, experience_level, education_level,
             job_description, company_name, status, is_valid, scraped_at, last_updated)
        VALUES (:job_url, :job_id, :job_title, :contract_type, :publication_date,
                :location, :city, :country, :region,
                :job_family, :experience_level, :education_level,
                :job_description, :company_name, 'Live', 1, :now, :now)
        ON CONFLICT(job_url) DO UPDATE SET
            job_title        = excluded.job_title,
            contract_type    = excluded.contract_type,
            publication_date = excluded.publication_date,
            location         = excluded.location,
            city             = excluded.city,
            country          = excluded.country,
            region           = excluded.region,
            job_family       = excluded.job_family,
            experience_level = excluded.experience_level,
            education_level  = excluded.education_level,
            job_description  = excluded.job_description,
            status           = 'Live',
            is_valid         = 1,
            scraped_at       = :now,
            last_updated     = :now
    """, {**job, "now": now})


def mark_expired(conn: sqlite3.Connection, urls: set[str]) -> int:
    if not urls:
        return 0
    ph = ",".join("?" * len(urls))
    conn.execute(f"UPDATE jobs SET status='Expired', last_updated=datetime('now') WHERE job_url IN ({ph})", list(urls))
    conn.commit()
    return len(urls)


def get_live_urls(conn: sqlite3.Connection) -> set[str]:
    return {r[0] for r in conn.execute("SELECT job_url FROM jobs WHERE status='Live'")}


# ══════════════════════════════════════════════════════════════════
# Scraping
# ══════════════════════════════════════════════════════════════════

def fetch_feed() -> list[dict]:
    """Récupère le JSON Feed Teamtailor."""
    logger.info(f"📥 Récupération du feed : {FEED_URL}")
    resp = requests.get(FEED_URL, headers=HEADERS, timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()
    data = resp.json()
    items = data.get("items", [])
    logger.info(f"   → {len(items)} offre(s) trouvée(s)")
    return items


def transform_item(item: dict) -> dict:
    """Convertit un item du feed en dict Taleos."""
    title   = (item.get("title") or "").strip()
    url     = (item.get("url") or "").strip()
    job_id  = _extract_job_id(url)

    # Date de publication
    pub_raw = item.get("date_published") or ""
    pub_date = pub_raw[:10] if pub_raw else ""

    # Description texte
    description = _html_to_text(item.get("content_html") or "")

    # Localisation depuis _jobposting
    job_posting = item.get("_jobposting") or {}
    cities = _extract_locations(job_posting)
    location, country, region = _resolve_country_region(cities)
    city = cities[0] if cities else ""

    # Contrat, expérience, éducation, famille
    contract_type    = _guess_contract(title)
    experience_level = extract_experience_level(title + " " + description)
    education_level  = extract_education_level(title + " " + description)
    job_family       = classify_job_family(title + " " + description)

    return {
        "job_url":          url,
        "job_id":           job_id,
        "job_title":        title,
        "contract_type":    contract_type,
        "publication_date": pub_date,
        "location":         location,
        "city":             city,
        "country":          country,
        "region":           region,
        "job_family":       job_family,
        "experience_level": experience_level,
        "education_level":  education_level,
        "job_description":  description[:8000],
        "company_name":     COMPANY_NAME,
    }


# ══════════════════════════════════════════════════════════════════
# Main
# ══════════════════════════════════════════════════════════════════

def main():
    t0 = time.time()
    conn = init_db(DB_PATH)

    # 1. Récupère toutes les offres
    try:
        items = fetch_feed()
    except Exception as e:
        logger.error(f"❌ Erreur lors de la récupération du feed : {e}")
        conn.close()
        return

    if not items:
        logger.warning("⚠️  Aucune offre trouvée — DB conservée telle quelle.")
        conn.close()
        return

    # 2. Transforme
    jobs = [transform_item(item) for item in items]
    live_urls_feed = {j["job_url"] for j in jobs if j["job_url"]}

    # 3. Marque les offres expirées (présentes en DB mais absentes du feed)
    live_in_db = get_live_urls(conn)
    expired_urls = live_in_db - live_urls_feed
    expired_count = mark_expired(conn, expired_urls)
    if expired_count:
        logger.info(f"🗑️  {expired_count} offre(s) expirée(s) (absentes du feed)")

    # 4. Upsert
    new_count = sum(1 for j in jobs if j["job_url"] not in live_in_db)
    for job in jobs:
        upsert_job(conn, job)
    conn.commit()

    total_live = conn.execute("SELECT COUNT(*) FROM jobs WHERE status='Live'").fetchone()[0]
    elapsed = time.time() - t0

    logger.info(f"\n{'='*60}")
    logger.info(f"✅ {COMPANY_NAME} — {total_live} offres Live en base")
    logger.info(f"   Nouvelles  : {new_count}")
    logger.info(f"   Expirées   : {expired_count}")
    logger.info(f"   Durée      : {elapsed:.1f}s")
    logger.info(f"   Base       : {DB_PATH}")
    logger.info(f"{'='*60}")

    conn.close()


if __name__ == "__main__":
    main()
