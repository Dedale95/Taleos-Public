#!/usr/bin/env python3
"""
Lloyds Banking Group — Scraper Workday
========================================
Source : lbg.wd3.myworkdayjobs.com / LBG_Careers
API    : POST /wday/cxs/lbg/LBG_Careers/jobs (listing)
         GET  /wday/cxs/lbg/LBG_Careers/{path} (détail)
Scope  : ~93 offres — principalement UK (Lloyds, Halifax, Bank of Scotland…)

Schéma DB : standard Taleos
"""
from __future__ import annotations

import asyncio
import logging
import re
import sqlite3
from datetime import datetime
from pathlib import Path

import aiohttp

# ───────────────────────────────── Logging ──────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(Path(__file__).parent / "lloyds_scraper.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger(__name__)

# ────────────────────────────────── Config ───────────────────────────────────
DB_PATH       = Path(__file__).parent / "lloyds_jobs.db"
WD_HOST       = "lbg.wd3.myworkdayjobs.com"
WD_TENANT     = "lbg"
WD_SITE       = "LBG_Careers"
API_BASE      = f"https://{WD_HOST}/wday/cxs/{WD_TENANT}/{WD_SITE}"
JOB_BASE_URL  = f"https://{WD_HOST}/{WD_SITE}"
PAGE_SIZE     = 20
CONCURRENCY   = 6
DELAY_LISTING = 0.4
TIMEOUT       = aiohttp.ClientTimeout(total=25)
USER_AGENT    = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)

# ──────────────────────────── Correspondances pays ───────────────────────────
COUNTRY_MAP: dict[str, tuple[str, str]] = {
    "united kingdom":           ("Royaume-Uni",          "Europe"),
    "united states of america": ("États-Unis",           "Amérique du Nord"),
    "united states":            ("États-Unis",           "Amérique du Nord"),
    "canada":                   ("Canada",               "Amérique du Nord"),
    "germany":                  ("Allemagne",            "Europe"),
    "france":                   ("France",               "Europe"),
    "netherlands":              ("Pays-Bas",             "Europe"),
    "ireland":                  ("Irlande",              "Europe"),
    "spain":                    ("Espagne",              "Europe"),
    "italy":                    ("Italie",               "Europe"),
    "poland":                   ("Pologne",              "Europe"),
    "luxembourg":               ("Luxembourg",           "Europe"),
    "india":                    ("Inde",                 "Asie-Pacifique"),
    "singapore":                ("Singapour",            "Asie-Pacifique"),
    "hong kong":                ("Hong Kong",            "Asie-Pacifique"),
    "hong kong sar":            ("Hong Kong",            "Asie-Pacifique"),
    "australia":                ("Australie",            "Asie-Pacifique"),
    "united arab emirates":     ("Émirats arabes unis",  "Moyen-Orient / Afrique"),
}

# Villes UK → entités Lloyds Banking Group
LBG_BRANDS: dict[str, str] = {
    # La plupart des offres Lloyds restent sous "Lloyds Banking Group"
    # sauf quelques marques explicites dans le titre
}

# ─────────────────────── Classification niveau d'expérience ─────────────────
LEVEL_PATTERNS: list[tuple[re.Pattern, str]] = [
    (re.compile(r"\b(managing director|md)\b",           re.I), "11 ans et plus"),
    (re.compile(r"\b(executive director|ed)\b",           re.I), "11 ans et plus"),
    (re.compile(r"\b(director)\b",                        re.I), "11 ans et plus"),
    (re.compile(r"\b(senior vice president|svp)\b",       re.I), "11 ans et plus"),
    (re.compile(r"\b(vice president|vp)\b",               re.I), "6 - 10 ans"),
    (re.compile(r"\b(senior manager|head of)\b",          re.I), "6 - 10 ans"),
    (re.compile(r"\b(avp)\b",                             re.I), "3 - 5 ans"),
    (re.compile(r"\b(senior analyst|senior associate)\b", re.I), "3 - 5 ans"),
    (re.compile(r"\b(manager)\b",                         re.I), "3 - 5 ans"),
    (re.compile(r"\b(associate)\b",                       re.I), "3 - 5 ans"),
    (re.compile(r"\b(analyst)\b",                         re.I), "0 - 2 ans"),
    (re.compile(r"\b(intern|internship|graduate|trainee|apprentice|placement)\b", re.I), "0 - 2 ans"),
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
            company_name        TEXT DEFAULT 'Lloyds Banking Group',
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
    return COUNTRY_MAP.get(key, (raw.title() if raw else "Non spécifié", "Europe"))


def _build_location(city: str | None, addl: list | None, country_fr: str) -> str:
    parts = []
    if city:
        parts.append(city.split(",")[0].strip())
    if addl:
        for a in (addl or []):
            loc = a.split(",")[0].strip() if isinstance(a, str) else ""
            if loc and loc not in parts:
                parts.append(loc)
    city_str = " / ".join(parts[:3]) if parts else ""
    if city_str and country_fr:
        return f"{city_str} - {country_fr}"
    return country_fr or city_str or ""


async def _fetch_json(session: aiohttp.ClientSession, url: str,
                       payload: dict | None = None, retries: int = 3) -> dict | None:
    for attempt in range(retries):
        try:
            if payload is not None:
                async with session.post(url, json=payload, timeout=TIMEOUT) as r:
                    if r.status == 200:
                        return await r.json()
            else:
                async with session.get(url, timeout=TIMEOUT) as r:
                    if r.status == 200:
                        return await r.json()
        except Exception as e:
            logger.debug(f"Attempt {attempt+1}/{retries} failed for {url}: {e}")
        if attempt < retries - 1:
            await asyncio.sleep(1.5 * (attempt + 1))
    return None


# ──────────────────────────── Phase 1 : listing ──────────────────────────────
async def fetch_all_listings(session: aiohttp.ClientSession) -> list[dict]:
    payload = {"appliedFacets": {}, "limit": PAGE_SIZE, "offset": 0, "searchText": ""}
    first = await _fetch_json(session, f"{API_BASE}/jobs", payload)
    if not first:
        logger.error("Impossible de contacter l'API Workday Lloyds")
        return []

    total = first.get("total", 0)
    logger.info(f"Phase 1 — {total} offres Lloyds à récupérer")
    all_postings = list(first.get("jobPostings", []))

    for offset in range(PAGE_SIZE, total, PAGE_SIZE):
        await asyncio.sleep(DELAY_LISTING)
        payload = {"appliedFacets": {}, "limit": PAGE_SIZE, "offset": offset, "searchText": ""}
        page = await _fetch_json(session, f"{API_BASE}/jobs", payload)
        if page:
            all_postings.extend(page.get("jobPostings", []))

    logger.info(f"Phase 1 terminée — {len(all_postings)} offres collectées")
    return all_postings


# ──────────────────────────── Phase 2 : détails ──────────────────────────────
async def fetch_detail(session: aiohttp.ClientSession,
                        sem: asyncio.Semaphore,
                        posting: dict) -> dict | None:
    ext_path = posting.get("externalPath", "")
    if not ext_path:
        return None
    url = f"{API_BASE}{ext_path}"
    async with sem:
        data = await _fetch_json(session, url)
    if not data:
        return None

    jd = data.get("jobPostingInfo") or {}

    # Pays
    country_raw = ""
    country_obj = jd.get("country")
    if isinstance(country_obj, dict):
        country_raw = country_obj.get("descriptor", "")
    elif isinstance(country_obj, str):
        country_raw = country_obj
    country_fr, region = _normalize_country(country_raw)

    city_raw   = jd.get("location") or ""
    addl_locs  = jd.get("additionalLocations") or []
    location   = _build_location(city_raw, addl_locs, country_fr)

    # Date
    posted_on = (posting.get("postedOn") or jd.get("postedOn") or "")[:10]
    if not posted_on:
        posted_on = datetime.utcnow().strftime("%Y-%m-%d")

    # Contrat
    time_type = (posting.get("timeType") or jd.get("timeType") or "").lower()
    title = jd.get("title") or posting.get("title") or ""
    title_l = title.lower()
    if any(k in title_l for k in ["intern", "internship", "apprentice", "placement", "trainee", "graduate programme"]):
        contract = "Stage"
    elif "part" in time_type:
        contract = "CDD"
    else:
        contract = "CDI"

    # Description
    desc_html = jd.get("jobDescription") or ""
    desc_text = re.sub(r"<[^>]+>", " ", desc_html)
    desc_text = re.sub(r"\s+", " ", desc_text).strip()[:25_000]

    # Niveau
    exp = _classify_level(title)

    # ID Workday
    job_id = re.sub(r"[^0-9]", "", ext_path.split("-")[-1] if "-" in ext_path else ext_path) or ext_path.split("/")[-1]

    # Détecter la marque (Lloyds, Halifax, Bank of Scotland…)
    company_name = "Lloyds Banking Group"
    desc_lower = desc_text.lower()
    if "halifax" in desc_lower or "halifax" in title_l:
        company_name = "Halifax (Lloyds Banking Group)"
    elif "bank of scotland" in desc_lower or "bank of scotland" in title_l:
        company_name = "Bank of Scotland (Lloyds Banking Group)"
    elif "scottish widows" in title_l:
        company_name = "Scottish Widows (Lloyds Banking Group)"

    job_url = f"{JOB_BASE_URL}{ext_path}"

    return {
        "job_url":          job_url,
        "job_id":           job_id,
        "job_title":        title,
        "contract_type":    contract,
        "publication_date": posted_on,
        "location":         location,
        "country":          country_fr,
        "region":           region,
        "job_family":       "",
        "experience_level": exp,
        "education_level":  "",
        "job_description":  desc_text,
        "company_name":     company_name,
        "status":           "Live",
    }


# ────────────────────────────────── Main ─────────────────────────────────────
async def main() -> None:
    logger.info("=== Lloyds Banking Group Scraper ===")
    conn = init_db(DB_PATH)
    existing_urls = get_existing_urls(conn)
    logger.info(f"DB existante : {len(existing_urls)} offres")

    headers = {
        "User-Agent": USER_AGENT,
        "Accept": "application/json",
        "Content-Type": "application/json",
    }
    connector = aiohttp.TCPConnector(limit=CONCURRENCY + 4, ssl=False)
    async with aiohttp.ClientSession(headers=headers, connector=connector) as session:
        postings = await fetch_all_listings(session)
        if not postings:
            logger.error("Aucune offre — abandon")
            conn.close()
            return

        api_urls = {f"{JOB_BASE_URL}{p['externalPath']}" for p in postings if p.get("externalPath")}
        newly_expired = existing_urls - api_urls
        if newly_expired:
            logger.info(f"  → {len(newly_expired)} offres disparues → Expired")
            mark_expired(conn, newly_expired)

        logger.info(f"Phase 2 — Fetch détails pour {len(postings)} offres…")
        sem = asyncio.Semaphore(CONCURRENCY)
        tasks = [fetch_detail(session, sem, p) for p in postings]
        results, done = [], 0
        for coro in asyncio.as_completed(tasks):
            job = await coro
            if job:
                results.append(job)
            done += 1

    saved = upsert_jobs(conn, results)
    total_live = conn.execute(
        "SELECT COUNT(*) FROM jobs WHERE is_valid=1 AND status='Live'"
    ).fetchone()[0]
    conn.close()
    logger.info(f"✅ Lloyds — {saved} offres upsert, {total_live} offres Live en base")


if __name__ == "__main__":
    asyncio.run(main())
