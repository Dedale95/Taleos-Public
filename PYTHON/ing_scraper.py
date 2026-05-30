#!/usr/bin/env python3
"""
ING — Scraper Radancy (careers.ing.com)
=========================================
Source  : careers.ing.com/en/search-jobs  (Radancy HTML, 15 offres/page)
Détails : careers.ing.com/en/job/{city}/{slug}/{cat-id}/{job-id}  (JSON-LD)
Scope   : ~859 offres mondiales (NL, BE, PL, DE, APAC, MENA…)

Phase 1 : parse HTML de chaque page de listing → liste d'URLs
Phase 2 : fetch JSON-LD de chaque page de détail → date, description, localisation
"""
from __future__ import annotations

import asyncio
import json
import logging
import re
import sqlite3
from datetime import datetime
from html import unescape
from pathlib import Path
from typing import Optional

import aiohttp
from bs4 import BeautifulSoup

# ───────────────────────────────── Logging ──────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(Path(__file__).parent / "ing_scraper.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger(__name__)

# ────────────────────────────────── Config ───────────────────────────────────
DB_PATH        = Path(__file__).parent / "ing_jobs.db"
BASE_URL       = "https://careers.ing.com"
SEARCH_URL     = f"{BASE_URL}/en/search-jobs"
PAGE_SIZE_HTML = 15         # Radancy retourne 15 offres par page
CONCURRENCY    = 8
DELAY_LISTING  = 0.5
DELAY_DETAIL   = 0.3
TIMEOUT        = aiohttp.ClientTimeout(total=30)
USER_AGENT     = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)

# ──────────────────────────── Correspondances pays ───────────────────────────
# ING est surtout Pays-Bas, Belgique, Pologne, Allemagne + international
COUNTRY_MAP: dict[str, tuple[str, str]] = {
    "netherlands":              ("Pays-Bas",             "Europe"),
    "belgium":                  ("Belgique",             "Europe"),
    "poland":                   ("Pologne",              "Europe"),
    "germany":                  ("Allemagne",            "Europe"),
    "france":                   ("France",               "Europe"),
    "spain":                    ("Espagne",              "Europe"),
    "italy":                    ("Italie",               "Europe"),
    "united kingdom":           ("Royaume-Uni",          "Europe"),
    "luxembourg":               ("Luxembourg",           "Europe"),
    "ireland":                  ("Irlande",              "Europe"),
    "romania":                  ("Roumanie",             "Europe"),
    "czech republic":           ("République tchèque",   "Europe"),
    "hungary":                  ("Hongrie",              "Europe"),
    "austria":                  ("Autriche",             "Europe"),
    "portugal":                 ("Portugal",             "Europe"),
    "turkey":                   ("Turquie",              "Europe"),
    "united states of america": ("États-Unis",           "Amérique du Nord"),
    "united states":            ("États-Unis",           "Amérique du Nord"),
    "canada":                   ("Canada",               "Amérique du Nord"),
    "australia":                ("Australie",            "Asie-Pacifique"),
    "singapore":                ("Singapour",            "Asie-Pacifique"),
    "india":                    ("Inde",                 "Asie-Pacifique"),
    "china":                    ("Chine",                "Asie-Pacifique"),
    "hong kong":                ("Hong Kong",            "Asie-Pacifique"),
    "japan":                    ("Japon",                "Asie-Pacifique"),
    "philippines":              ("Philippines",          "Asie-Pacifique"),
    "thailand":                 ("Thaïlande",            "Asie-Pacifique"),
    "korea, republic of":       ("Corée du Sud",         "Asie-Pacifique"),
    "south korea":              ("Corée du Sud",         "Asie-Pacifique"),
    "taiwan":                   ("Taïwan",               "Asie-Pacifique"),
    "united arab emirates":     ("Émirats arabes unis",  "Moyen-Orient / Afrique"),
    "turkey":                   ("Turquie",              "Moyen-Orient / Afrique"),
}

# Villes → (pays_fr, région) fallback quand le pays n'est pas explicite dans JSON-LD
CITY_TO_COUNTRY: dict[str, tuple[str, str]] = {
    "amsterdam":    ("Pays-Bas",  "Europe"),
    "amsterdam - cedar": ("Pays-Bas", "Europe"),
    "amsterdam-cedar": ("Pays-Bas", "Europe"),
    "rotterdam":    ("Pays-Bas",  "Europe"),
    "eindhoven":    ("Pays-Bas",  "Europe"),
    "brussels":     ("Belgique",  "Europe"),
    "bruxelles":    ("Belgique",  "Europe"),
    "warsaw":       ("Pologne",   "Europe"),
    "katowice":     ("Pologne",   "Europe"),
    "wroclaw":      ("Pologne",   "Europe"),
    "poznan":       ("Pologne",   "Europe"),
    "frankfurt":    ("Allemagne", "Europe"),
    "berlin":       ("Allemagne", "Europe"),
    "paris":        ("France",    "Europe"),
    "london":       ("Royaume-Uni","Europe"),
    "madrid":       ("Espagne",   "Europe"),
    "milan":        ("Italie",    "Europe"),
    "bucharest":    ("Roumanie",  "Europe"),
    "singapore":    ("Singapour", "Asie-Pacifique"),
    "hong kong":    ("Hong Kong", "Asie-Pacifique"),
    "shanghai":     ("Chine",     "Asie-Pacifique"),
    "beijing":      ("Chine",     "Asie-Pacifique"),
    "tokyo":        ("Japon",     "Asie-Pacifique"),
    "manila":       ("Philippines","Asie-Pacifique"),
    "bangkok":      ("Thaïlande", "Asie-Pacifique"),
    "dubai":        ("Émirats arabes unis", "Moyen-Orient / Afrique"),
    "istanbul":     ("Turquie",   "Moyen-Orient / Afrique"),
    "new york":     ("États-Unis","Amérique du Nord"),
    "sydney":       ("Australie", "Asie-Pacifique"),
    "mumbai":       ("Inde",      "Asie-Pacifique"),
    "bangalore":    ("Inde",      "Asie-Pacifique"),
}

# ─────────────────────── Classification niveau d'expérience ─────────────────
LEVEL_PATTERNS: list[tuple[re.Pattern, str]] = [
    (re.compile(r"\b(managing director|head of|cio|cto|cfo)\b", re.I), "11 ans et plus"),
    (re.compile(r"\b(director)\b",                                re.I), "11 ans et plus"),
    (re.compile(r"\b(senior vice president|svp)\b",               re.I), "11 ans et plus"),
    (re.compile(r"\b(vice president|vp)\b",                       re.I), "6 - 10 ans"),
    (re.compile(r"\b(lead|principal|expert)\b",                   re.I), "6 - 10 ans"),
    (re.compile(r"\b(senior|sr\.)\b",                             re.I), "3 - 5 ans"),
    (re.compile(r"\b(manager)\b",                                 re.I), "3 - 5 ans"),
    (re.compile(r"\b(analyst|associate|officer)\b",               re.I), "0 - 2 ans"),
    (re.compile(r"\b(intern|internship|trainee|junior|graduate|stage|apprentice)\b", re.I), "0 - 2 ans"),
]


def _classify_level(title: str) -> str:
    for pat, lvl in LEVEL_PATTERNS:
        if pat.search(title):
            return lvl
    return ""


# ──────────────────────────── Database ───────────────────────────────────────
def init_db(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS jobs (
            job_url             TEXT PRIMARY KEY,
            job_id              TEXT,
            job_title           TEXT,
            contract_type       TEXT,
            publication_date    TEXT,
            location            TEXT,
            country             TEXT,
            region              TEXT,
            job_family          TEXT,
            experience_level    TEXT,
            education_level     TEXT,
            job_description     TEXT,
            company_name        TEXT DEFAULT 'ING',
            status              TEXT DEFAULT 'Live',
            is_valid            INTEGER DEFAULT 1,
            first_seen          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            last_updated        TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.commit()
    return conn


def upsert_jobs(conn: sqlite3.Connection, jobs: list[dict]) -> int:
    now = datetime.utcnow().isoformat()
    rows = [
        (
            j["job_url"], j["job_id"], j["job_title"], j["contract_type"],
            j["publication_date"], j["location"], j["country"], j["region"],
            j["job_family"], j["experience_level"], j["education_level"],
            j["job_description"], j["company_name"], j["status"], now,
        )
        for j in jobs
    ]
    conn.executemany("""
        INSERT INTO jobs
            (job_url, job_id, job_title, contract_type, publication_date,
             location, country, region, job_family, experience_level,
             education_level, job_description, company_name, status, last_updated)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        ON CONFLICT(job_url) DO UPDATE SET
            job_title        = excluded.job_title,
            contract_type    = excluded.contract_type,
            publication_date = excluded.publication_date,
            location         = excluded.location,
            country          = excluded.country,
            region           = excluded.region,
            job_family       = excluded.job_family,
            experience_level = excluded.experience_level,
            education_level  = excluded.education_level,
            job_description  = excluded.job_description,
            status           = excluded.status,
            last_updated     = excluded.last_updated
    """, rows)
    conn.commit()
    return len(rows)


def get_existing_urls(conn: sqlite3.Connection) -> set[str]:
    return {r[0] for r in conn.execute("SELECT job_url FROM jobs WHERE is_valid=1")}


def mark_expired(conn: sqlite3.Connection, urls: set[str]) -> None:
    if not urls:
        return
    placeholders = ",".join("?" * len(urls))
    conn.execute(
        f"UPDATE jobs SET status='Expired', last_updated=CURRENT_TIMESTAMP "
        f"WHERE job_url IN ({placeholders})",
        list(urls),
    )
    conn.commit()


# ──────────────────────────── Helpers ────────────────────────────────────────
def _normalize_country(raw: str) -> tuple[str, str]:
    key = (raw or "").strip().lower()
    return COUNTRY_MAP.get(key, ("", ""))


def _city_country(city: str) -> tuple[str, str]:
    key = (city or "").split(",")[0].strip().lower()
    return CITY_TO_COUNTRY.get(key, ("", "Europe"))


def _parse_location_text(loc_text: str) -> tuple[str, str, str]:
    """
    Extrait (city, country_fr, region) depuis une chaîne type 'Warsaw, Poland'.
    """
    parts = [p.strip() for p in loc_text.split(",")]
    city = parts[0] if parts else ""
    country_raw = parts[-1] if len(parts) > 1 else ""
    country_fr, region = _normalize_country(country_raw)
    if not country_fr:
        country_fr, region = _city_country(city)
    if not country_fr:
        country_fr = country_raw
        region = "Europe"
    location = f"{city} - {country_fr}" if city and country_fr else country_fr or city
    return location, country_fr, region


async def _fetch_html(session: aiohttp.ClientSession, url: str,
                       retries: int = 3) -> str | None:
    for attempt in range(retries):
        try:
            async with session.get(url, timeout=TIMEOUT) as r:
                if r.status == 200:
                    return await r.text()
        except Exception as e:
            logger.debug(f"HTML fetch attempt {attempt+1}/{retries} {url}: {e}")
        if attempt < retries - 1:
            await asyncio.sleep(1.5 * (attempt + 1))
    return None


# ──────────────────────────── Phase 1 : listing HTML ─────────────────────────
def _parse_listing_page(html: str) -> list[dict]:
    """Parse une page de listing Radancy et retourne les jobs avec URL + location."""
    jobs = []
    # Pattern : href="/en/job/{city}/{slug}/{cat-id}/{job-id}" data-job-id="{id}"
    pattern = re.compile(
        r'href="(/en/job/([^/]+)/([^/]+)/(\d+)/(\d+))"[^>]*data-job-id="(\d+)"[^>]*>'
        r'\s*<h2[^>]*>\s*([^<]+)\s*</h2>'
        r'.*?<span class="job-location">([^<]+)</span>',
        re.DOTALL,
    )
    for m in pattern.finditer(html):
        rel_url, city_slug, title_slug, cat_id, job_id_url, job_id, title_raw, loc_raw = m.groups()
        title = unescape(title_raw.strip())
        location_text = unescape(loc_raw.strip())
        jobs.append({
            "job_url":       f"{BASE_URL}{rel_url}",
            "job_id":        job_id,
            "job_title":     title,
            "location_text": location_text,
            "city_slug":     city_slug,
        })
    return jobs


async def fetch_all_listings(session: aiohttp.ClientSession) -> list[dict]:
    # Page 1 → nombre total d'offres
    html1 = await _fetch_html(session, f"{SEARCH_URL}?p=1")
    if not html1:
        logger.error("Impossible de charger la page de listing ING")
        return []

    total_match = re.search(r"(\d+)\s+jobs?", html1, re.I)
    total = int(total_match.group(1)) if total_match else 0
    n_pages = max(1, (total + PAGE_SIZE_HTML - 1) // PAGE_SIZE_HTML)
    logger.info(f"Phase 1 — {total} offres ING, {n_pages} pages")

    all_jobs = _parse_listing_page(html1)
    logger.info(f"  Page 1 → {len(all_jobs)} offres")

    for page_num in range(2, n_pages + 1):
        await asyncio.sleep(DELAY_LISTING)
        html = await _fetch_html(session, f"{SEARCH_URL}?p={page_num}")
        if html:
            jobs_page = _parse_listing_page(html)
            all_jobs.extend(jobs_page)
            if page_num % 10 == 0:
                logger.info(f"  Page {page_num}/{n_pages} → {len(all_jobs)} offres cumulées")

    logger.info(f"Phase 1 terminée — {len(all_jobs)} offres collectées")
    return all_jobs


# ──────────────────────────── Phase 2 : détails JSON-LD ──────────────────────
async def fetch_detail(session: aiohttp.ClientSession,
                        sem: asyncio.Semaphore,
                        job: dict) -> dict | None:
    async with sem:
        await asyncio.sleep(DELAY_DETAIL)
        html = await _fetch_html(session, job["job_url"])
    if not html:
        # Utiliser les données de listing en fallback
        loc, country_fr, region = _parse_location_text(job.get("location_text", ""))
        return {**job, "location": loc, "country": country_fr, "region": region,
                "publication_date": datetime.utcnow().strftime("%Y-%m-%d"),
                "contract_type": "CDI", "job_family": "", "experience_level": "",
                "education_level": "", "job_description": "", "company_name": "ING", "status": "Live"}

    # JSON-LD
    jld_matches = re.findall(r'<script[^>]+application/ld\+json[^>]*>(.*?)</script>', html, re.DOTALL)
    jld = {}
    for jl in jld_matches:
        try:
            d = json.loads(jl)
            if d.get("@type") == "JobPosting":
                jld = d
                break
        except Exception:
            pass

    # Date de publication
    raw_date = jld.get("datePosted") or ""
    # Format "2026-5-6" → "2026-05-06"
    pub_date = ""
    if raw_date:
        parts = raw_date.split("-")
        if len(parts) == 3:
            pub_date = f"{parts[0]}-{parts[1].zfill(2)}-{parts[2].zfill(2)}"
    if not pub_date:
        pub_date = datetime.utcnow().strftime("%Y-%m-%d")

    # Localisation depuis JSON-LD ou fallback listing
    jld_loc = jld.get("jobLocation")
    city, country_fr, region = "", "", ""
    if isinstance(jld_loc, dict):
        addr = jld_loc.get("address") or {}
        city = addr.get("addressLocality") or addr.get("addressRegion") or ""
        country_raw = addr.get("addressCountry") or ""
        country_fr, region = _normalize_country(country_raw)
    elif isinstance(jld_loc, list) and jld_loc:
        addr = (jld_loc[0].get("address") or {})
        city = addr.get("addressLocality") or ""
        country_raw = addr.get("addressCountry") or ""
        country_fr, region = _normalize_country(country_raw)

    if not country_fr:
        city_slug = job.get("city_slug", "").replace("-", " ")
        country_fr, region = _city_country(city_slug)
        if not city:
            city = city_slug.title()

    # Fallback sur location_text du listing
    if not country_fr:
        loc_text = job.get("location_text", "")
        _, country_fr, region = _parse_location_text(loc_text)
        if not city:
            city = loc_text.split(",")[0].strip()

    location = f"{city} - {country_fr}" if city and country_fr else (country_fr or city or "")

    # Type de contrat depuis JSON-LD ou titre
    emp_type = (jld.get("employmentType") or "").upper()
    title = job["job_title"]
    title_l = title.lower()
    if any(k in title_l for k in ["intern", "internship", "stage", "trainee", "apprentice", "graduate"]):
        contract = "Stage"
    elif "alternance" in title_l or "apprentissage" in title_l:
        contract = "Alternance"
    elif "part" in emp_type or "part" in title_l:
        contract = "CDD"
    else:
        contract = "CDI"

    # Description (JSON-LD contient du HTML)
    desc_html = jld.get("description") or ""
    desc_text = re.sub(r"<[^>]+>", " ", desc_html)
    desc_text = re.sub(r"&[a-z]+;", " ", desc_text)
    desc_text = re.sub(r"\s+", " ", desc_text).strip()[:25_000]

    # Niveau d'expérience
    exp = _classify_level(title)

    return {
        "job_url":          job["job_url"],
        "job_id":           job["job_id"],
        "job_title":        title,
        "contract_type":    contract,
        "publication_date": pub_date,
        "location":         location,
        "country":          country_fr,
        "region":           region,
        "job_family":       "",
        "experience_level": exp,
        "education_level":  "",
        "job_description":  desc_text,
        "company_name":     "ING",
        "status":           "Live",
    }


# ────────────────────────────────── Main ─────────────────────────────────────
async def main() -> None:
    logger.info("=== ING Scraper (Radancy) ===")
    conn = init_db(DB_PATH)
    existing_urls = get_existing_urls(conn)
    logger.info(f"DB existante : {len(existing_urls)} offres")

    headers = {
        "User-Agent": USER_AGENT,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
    }
    connector = aiohttp.TCPConnector(limit=CONCURRENCY + 4, ssl=False)
    async with aiohttp.ClientSession(headers=headers, connector=connector) as session:
        # Phase 1
        all_listings = await fetch_all_listings(session)
        if not all_listings:
            logger.error("Aucune offre collectée — abandon")
            conn.close()
            return

        api_urls = {j["job_url"] for j in all_listings}
        newly_expired = existing_urls - api_urls
        if newly_expired:
            logger.info(f"  → {len(newly_expired)} offres disparues → Expired")
            mark_expired(conn, newly_expired)

        # Phase 2 : fetch détails en concurrence
        logger.info(f"Phase 2 — Fetch détails pour {len(all_listings)} offres…")
        sem = asyncio.Semaphore(CONCURRENCY)
        tasks = [fetch_detail(session, sem, j) for j in all_listings]
        results, done = [], 0
        for coro in asyncio.as_completed(tasks):
            job = await coro
            if job:
                results.append(job)
            done += 1
            if done % 100 == 0:
                logger.info(f"  Détails : {done}/{len(all_listings)}")

    saved = upsert_jobs(conn, results)
    total_live = conn.execute(
        "SELECT COUNT(*) FROM jobs WHERE is_valid=1 AND status='Live'"
    ).fetchone()[0]
    conn.close()
    logger.info(f"✅ ING — {saved} offres upsert, {total_live} offres Live en base")


if __name__ == "__main__":
    asyncio.run(main())
