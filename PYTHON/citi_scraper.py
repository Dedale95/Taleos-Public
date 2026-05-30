#!/usr/bin/env python3
"""
Citi Bank jobs scraper
Source: jobs.citi.com (Radancy TalentBrew platform)
Covers: All regions (North America, EMEA, APAC, Latin America)
        All levels (Professional, Entry Level, Executive, Student & Grad Programs)
"""

import asyncio
import json
import logging
import re
import sqlite3
import time
from datetime import datetime, timezone
from html import unescape
from pathlib import Path

import aiohttp
from bs4 import BeautifulSoup

# ─── Paths ────────────────────────────────────────────────────────────────────
SCRIPT_DIR = Path(__file__).parent
DB_PATH = SCRIPT_DIR / "citi_jobs.db"
LOG_PATH = SCRIPT_DIR / "citi_scraper.log"

# ─── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_PATH, encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger(__name__)

# ─── Constants ────────────────────────────────────────────────────────────────
BASE_URL = "https://jobs.citi.com"
SEARCH_URL = f"{BASE_URL}/search-jobs"
PAGE_SIZE = 15  # Citi shows 15 results per page
MAX_CONCURRENT = 8  # simultaneous detail page fetches
DELAY_BETWEEN_PAGES = 1.0  # seconds between listing page fetches
DELAY_DETAIL = 0.3  # seconds between detail fetches in the pool

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
}

# ─── Job Family Group → canonical family mapping ──────────────────────────────
# Based on Citi's actual JobFamilyGroupFacet values
JFG_MAP: dict[str, str] = {
    # Technology
    "technology": "Développement & Architecture",
    # Consumer / Retail
    "consumer sales": "Conseil Clientèle Particuliers",
    "consumer banking": "Conseil Clientèle Particuliers",
    "customer service": "Conseil Clientèle Particuliers",
    "branch banking": "Conseil Clientèle Particuliers",
    # Operations
    "operations - transaction services": "Gestion des opérations",
    "operations - services": "Gestion des opérations",
    "operations - core": "Gestion des opérations",
    "operations": "Gestion des opérations",
    # Risk
    "risk management": "Risques / Contrôles permanents",
    "controls governance & oversight": "Risques / Contrôles permanents",
    "compliance and control": "Conformité / Sécurité financière",
    # Private Bank / Wealth
    "private client coverage": "Banque Privée & Gestion de Patrimoine",
    "private banking": "Banque Privée & Gestion de Patrimoine",
    # Product / PMO
    "product management and development": "Organisation / Qualité",
    "project and program management": "Organisation / Qualité",
    # Finance
    "finance": "Finances / Comptabilité / Contrôle de gestion",
    "accounting": "Finances / Comptabilité / Contrôle de gestion",
    # Institutional / Corporate Banking
    "institutional banking": "Conseil Clientèle Entreprises",
    "commercial and business sales": "Développement Commercial",
    # Markets / Trading
    "institutional sales": "Marchés financiers & Trading",
    "institutional trading": "Marchés financiers & Trading",
    "markets": "Marchés financiers & Trading",
    "securities services": "Gestion des opérations",
    # Investment Banking
    "investment banking": "Financement et Investissement",
    "banking": "Financement et Investissement",
    # Analytics / Decision
    "decision management": "Data & Intelligence Artificielle",
    "data": "Data & Intelligence Artificielle",
    # HR
    "human resources": "Ressources Humaines",
    # Legal
    "legal": "Juridique",
    # Audit
    "internal audit": "Inspection / Audit",
    "audit": "Inspection / Audit",
    # General / Other
    "corporate": "Direction générale",
    "communications": "Marketing et Communication",
    "marketing": "Marketing et Communication",
    "enterprise services": "Achat",
    "real estate": "Immobilier",
    "insurance": "Assurances",
}

# Job Family (sub-category) → canonical family — more precise when JFG is ambiguous
JF_MAP: dict[str, str] = {
    "applications development": "Développement & Architecture",
    "digital software engineering": "Développement & Architecture",
    "software engineering": "Développement & Architecture",
    "applications support": "Infrastructure & Cloud",
    "technology solutions": "Développement & Architecture",
    "database administration": "Infrastructure & Cloud",
    "infrastructure": "Infrastructure & Cloud",
    "cybersecurity": "Cybersécurité",
    "information security": "Cybersécurité",
    "data management": "Data & Intelligence Artificielle",
    "data engineering": "Data & Intelligence Artificielle",
    "data science": "Data & Intelligence Artificielle",
    "analytics": "Data & Intelligence Artificielle",
    "enterprise data": "Data & Intelligence Artificielle",
    "securities and derivatives processing": "Gestion des opérations",
    "asset servicing": "Gestion des opérations",
    "operations support": "Gestion des opérations",
    "treasury operations": "Gestion des opérations",
    "trade finance operations": "Gestion des opérations",
    "credit risk": "Risques / Contrôles permanents",
    "market risk": "Risques / Contrôles permanents",
    "operational risk": "Risques / Contrôles permanents",
    "risk management": "Risques / Contrôles permanents",
    "compliance": "Conformité / Sécurité financière",
    "anti money laundering": "Conformité / Sécurité financière",
    "know your customer": "Conformité / Sécurité financière",
    "financial planning services": "Banque Privée & Gestion de Patrimoine",
    "private banker": "Banque Privée & Gestion de Patrimoine",
    "wealth management": "Banque Privée & Gestion de Patrimoine",
    "relationship management": "Conseil Clientèle Entreprises",
    "investment banking": "Financement et Investissement",
    "capital markets": "Marchés financiers & Trading",
    "equities": "Marchés financiers & Trading",
    "fixed income": "Marchés financiers & Trading",
    "foreign exchange": "Marchés financiers & Trading",
    "derivatives": "Marchés financiers & Trading",
    "program management": "Organisation / Qualité",
    "project management": "Organisation / Qualité",
    "product management": "Organisation / Qualité",
    "branch sales": "Conseil Clientèle Particuliers",
    "branch service": "Conseil Clientèle Particuliers",
    "financial analysis": "Analyse financière et économique",
    "finance": "Finances / Comptabilité / Contrôle de gestion",
    "accounting": "Finances / Comptabilité / Contrôle de gestion",
    "controller": "Finances / Comptabilité / Contrôle de gestion",
    "human resources": "Ressources Humaines",
    "talent acquisition": "Ressources Humaines",
    "legal": "Juridique",
    "audit": "Inspection / Audit",
    "internal audit": "Inspection / Audit",
    "communications": "Marketing et Communication",
}

# ─── Experience level from title ──────────────────────────────────────────────
TITLE_LEVEL_PATTERNS = [
    # Interns / Students
    (r"\b(intern|internship|summer analyst|placement analyst|apprentice|placement student)\b", "Stagiaire / Alternant"),
    # Senior leadership
    (r"\b(managing director|md\b|chief\b|head of|global head|regional head)\b", "Directeur / Managing Director"),
    # Director / SVP
    (r"\b(director|senior vice president|svp\b|executive vice president|evp)\b", "Senior Vice President / Director"),
    # VP level
    (r"\b(vice president|\bvp\b)\b", "Vice President"),
    # Senior / Lead / Principal
    (r"\b(senior|lead\b|principal|staff engineer|specialist|expert)\b", "Manager / Associé Senior"),
    # Manager level
    (r"\b(manager|management)\b", "Manager / Associé Senior"),
    # Analyst / Associate / Officer
    (r"\b(analyst|associate|officer|consultant|advisor|adviser)\b", "Analyst / Associé"),
    # Engineers / Developers (if no other level keyword)
    (r"\b(engineer|developer|architect|scientist|quantitative)\b", "Manager / Associé Senior"),
    # Generic professional (last resort)
    (r"\b(specialist|coordinator|administrator)\b", "Analyst / Associé"),
]

CAREER_LEVEL_MAP = {
    "student and grad programs": "Stagiaire / Alternant",
    "entry level": "Analyst / Associé",
    "professional": "Vice President",
    "executive": "Directeur / Managing Director",
}

# ─── Contract type ─────────────────────────────────────────────────────────────
TIME_TYPE_MAP = {
    "full time": "CDI / Temps Plein",
    "part time": "Temps Partiel",
}

EMP_TYPE_MAP = {
    "intern": "Stage / Apprentissage",
    "contract": "CDD / Contrat",
    "temporary": "CDD / Contrat",
    "regular": "CDI / Temps Plein",
    "fixed term": "CDD / Contrat",
}

# ─── Country normalizer ────────────────────────────────────────────────────────
COUNTRY_MAP = {
    "united states": "États-Unis",
    "usa": "États-Unis",
    "us": "États-Unis",
    "united kingdom": "Royaume-Uni",
    "uk": "Royaume-Uni",
    "ireland": "Irlande",
    "france": "France",
    "germany": "Allemagne",
    "spain": "Espagne",
    "italy": "Italie",
    "poland": "Pologne",
    "netherlands": "Pays-Bas",
    "luxembourg": "Luxembourg",
    "switzerland": "Suisse",
    "sweden": "Suède",
    "norway": "Norvège",
    "denmark": "Danemark",
    "finland": "Finlande",
    "belgium": "Belgique",
    "portugal": "Portugal",
    "austria": "Autriche",
    "hungary": "Hongrie",
    "czech republic": "République Tchèque",
    "romania": "Roumanie",
    "bulgaria": "Bulgarie",
    "greece": "Grèce",
    "turkey": "Turquie",
    "russia": "Russie",
    "ukraine": "Ukraine",
    "united arab emirates": "Émirats Arabes Unis",
    "uae": "Émirats Arabes Unis",
    "saudi arabia": "Arabie Saoudite",
    "qatar": "Qatar",
    "bahrain": "Bahreïn",
    "south africa": "Afrique du Sud",
    "nigeria": "Nigeria",
    "kenya": "Kenya",
    "egypt": "Égypte",
    "india": "Inde",
    "china": "Chine",
    "hong kong": "Hong Kong",
    "singapore": "Singapour",
    "japan": "Japon",
    "south korea": "Corée du Sud",
    "korea": "Corée du Sud",
    "korea, republic of": "Corée du Sud",
    "republic of korea": "Corée du Sud",
    "costa rica": "Costa Rica",
    "bahamas": "Bahamas",
    "panama": "Panama",
    "peru": "Pérou",
    "nigeria": "Nigeria",
    "laos": "Laos",
    "kuwait": "Koweït",
    "australia": "Australie",
    "new zealand": "Nouvelle-Zélande",
    "canada": "Canada",
    "brazil": "Brésil",
    "mexico": "Mexique",
    "argentina": "Argentine",
    "chile": "Chili",
    "colombia": "Colombie",
    "peru": "Pérou",
    "philippines": "Philippines",
    "malaysia": "Malaisie",
    "indonesia": "Indonésie",
    "thailand": "Thaïlande",
    "vietnam": "Vietnam",
    "taiwan": "Taïwan",
    "sri lanka": "Sri Lanka",
    "pakistan": "Pakistan",
    "bangladesh": "Bangladesh",
}

REGION_MAP = {
    "états-unis": "Amérique du Nord",
    "canada": "Amérique du Nord",
    "mexique": "Amérique Latine",
    "brésil": "Amérique Latine",
    "argentine": "Amérique Latine",
    "chili": "Amérique Latine",
    "colombie": "Amérique Latine",
    "pérou": "Amérique Latine",
    "royaume-uni": "Europe",
    "irlande": "Europe",
    "france": "Europe",
    "allemagne": "Europe",
    "espagne": "Europe",
    "italie": "Europe",
    "pologne": "Europe",
    "pays-bas": "Europe",
    "luxembourg": "Europe",
    "suisse": "Europe",
    "suède": "Europe",
    "norvège": "Europe",
    "danemark": "Europe",
    "finlande": "Europe",
    "belgique": "Europe",
    "portugal": "Europe",
    "autriche": "Europe",
    "hongrie": "Europe",
    "république tchèque": "Europe",
    "roumanie": "Europe",
    "bulgarie": "Europe",
    "grèce": "Europe",
    "turquie": "Europe",
    "russie": "Europe",
    "ukraine": "Europe",
    "émirats arabes unis": "Moyen-Orient & Afrique",
    "arabie saoudite": "Moyen-Orient & Afrique",
    "qatar": "Moyen-Orient & Afrique",
    "bahreïn": "Moyen-Orient & Afrique",
    "afrique du sud": "Moyen-Orient & Afrique",
    "nigeria": "Moyen-Orient & Afrique",
    "kenya": "Moyen-Orient & Afrique",
    "égypte": "Moyen-Orient & Afrique",
    "inde": "Asie-Pacifique",
    "chine": "Asie-Pacifique",
    "hong kong": "Asie-Pacifique",
    "singapour": "Asie-Pacifique",
    "japon": "Asie-Pacifique",
    "corée du sud": "Asie-Pacifique",
    "costa rica": "Amérique Latine",
    "australie": "Asie-Pacifique",
    "nouvelle-zélande": "Asie-Pacifique",
    "philippines": "Asie-Pacifique",
    "malaisie": "Asie-Pacifique",
    "indonésie": "Asie-Pacifique",
    "thaïlande": "Asie-Pacifique",
    "vietnam": "Asie-Pacifique",
    "taïwan": "Asie-Pacifique",
    "sri lanka": "Asie-Pacifique",
    "pakistan": "Asie-Pacifique",
    "bangladesh": "Asie-Pacifique",
}


# ─── Database ─────────────────────────────────────────────────────────────────
def init_db(conn: sqlite3.Connection) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS jobs (
            id TEXT PRIMARY KEY,
            title TEXT,
            company TEXT DEFAULT 'Citi',
            url TEXT,
            location TEXT,
            city TEXT,
            country TEXT,
            region TEXT,
            job_family TEXT,
            job_family_group TEXT,
            contract_type TEXT,
            experience_level TEXT,
            remote_type TEXT,
            date_posted TEXT,
            description TEXT,
            scraped_at TEXT,
            status TEXT DEFAULT 'Live'
        )
    """)
    conn.commit()


def upsert_job(conn: sqlite3.Connection, job: dict) -> None:
    conn.execute("""
        INSERT INTO jobs (
            id, title, company, url, location, city, country, region,
            job_family, job_family_group, contract_type, experience_level,
            remote_type, date_posted, description, scraped_at, status
        ) VALUES (
            :id, :title, :company, :url, :location, :city, :country, :region,
            :job_family, :job_family_group, :contract_type, :experience_level,
            :remote_type, :date_posted, :description, :scraped_at, :status
        )
        ON CONFLICT(id) DO UPDATE SET
            title=excluded.title,
            url=excluded.url,
            location=excluded.location,
            city=excluded.city,
            country=excluded.country,
            region=excluded.region,
            job_family=excluded.job_family,
            job_family_group=excluded.job_family_group,
            contract_type=excluded.contract_type,
            experience_level=excluded.experience_level,
            remote_type=excluded.remote_type,
            date_posted=excluded.date_posted,
            description=excluded.description,
            scraped_at=excluded.scraped_at,
            status=excluded.status
    """, job)


# ─── Mapping helpers ───────────────────────────────────────────────────────────
def normalize_text(s: str) -> str:
    return unescape(s).strip()


def map_family(jfg: str, jf: str, title: str = "", desc: str = "") -> str:
    """Map Job Family Group + Job Family to canonical family."""
    jfg_lower = (jfg or "").lower().strip()
    jf_lower = (jf or "").lower().strip()

    # Try Job Family first (more specific)
    for key, canonical in JF_MAP.items():
        if key in jf_lower:
            return canonical

    # Then Job Family Group
    for key, canonical in JFG_MAP.items():
        if key in jfg_lower:
            return canonical

    # Fallback: use classifier
    from job_family_classifier import classify_job_family
    result = classify_job_family(title, desc)
    return result or "Autres"


def map_experience_level(title: str, career_level: str, jf: str) -> str:
    """Map title + career level to experience label."""
    title_lower = title.lower()
    jf_lower = jf.lower()

    # Career level override
    cl_lower = (career_level or "").lower()
    if "student" in cl_lower or "grad" in cl_lower:
        return "Stagiaire / Alternant"
    if "intern" in jf_lower or "intern" in title_lower:
        return "Stagiaire / Alternant"

    # Title-based
    for pattern, level in TITLE_LEVEL_PATTERNS:
        if re.search(pattern, title_lower):
            return level

    # Generic career level
    for key, level in CAREER_LEVEL_MAP.items():
        if key in cl_lower:
            return level

    return "Non spécifié"


def map_contract(time_type: str, emp_type: str, title: str) -> str:
    """Map time type + employment type to contract label."""
    tt_lower = (time_type or "").lower()
    et_lower = (emp_type or "").lower()
    title_lower = (title or "").lower()

    if "intern" in et_lower or "intern" in title_lower or "stagiaire" in title_lower:
        return "Stage / Apprentissage"
    if "contract" in et_lower or "temporary" in et_lower or "fixed term" in et_lower:
        return "CDD / Contrat"
    if "part time" in tt_lower:
        return "Temps Partiel"

    for key, val in TIME_TYPE_MAP.items():
        if key in tt_lower:
            return val

    for key, val in EMP_TYPE_MAP.items():
        if key in et_lower:
            return val

    return "CDI / Temps Plein"  # Default for Citi


def normalize_country(raw: str) -> tuple[str, str]:
    """Return (country_fr, region_fr)."""
    raw_lower = raw.strip().lower()
    country_fr = COUNTRY_MAP.get(raw_lower, raw.strip())
    region_fr = REGION_MAP.get(country_fr.lower(), "Autres")
    return country_fr, region_fr


def map_remote(remote_type: str) -> str:
    """Map remote type to normalized label."""
    rt_lower = (remote_type or "").lower()
    if "hybrid" in rt_lower:
        return "Hybride"
    if "remote" in rt_lower:
        return "Télétravail"
    if "on-site" in rt_lower or "resident" in rt_lower:
        return "Présentiel"
    return "Non spécifié"


# ─── HTML parsing helpers ──────────────────────────────────────────────────────
def _extract_field(html: str, field_name: str) -> str:
    """Extract a structured field value from Citi job detail HTML."""
    escaped = re.escape(field_name)
    # Pattern handles both:
    # - "Field: </b></h2>VALUE..."
    # - "Field: </span></b></p>VALUE..."
    m = re.search(
        rf'{escaped}[:\s]*(?:</[^>]+>)+([^<\n]{{2,120}})',
        html,
    )
    if m:
        return normalize_text(m.group(1))
    return ""


def parse_listing_page(html: str) -> list[dict]:
    """Parse a search listing page and return basic job info."""
    jobs = []
    items = re.findall(
        r'<li[^>]*class="[^"]*job-item[^"]*"[^>]*>(.*?)</li>',
        html, re.S
    )
    for item in items:
        job_id_m = re.search(r'data-job-id="(\d+)"', item)
        if not job_id_m:
            continue
        job_id = job_id_m.group(1)

        href_m = re.search(r'href="(/job/[^"]+)"', item)
        url = BASE_URL + href_m.group(1) if href_m else ""

        title_m = re.search(
            r'class="sr-job-item__title"[^>]*>.*?<a[^>]*>([^<]+)</a>',
            item, re.S
        )
        title = normalize_text(title_m.group(1)) if title_m else ""

        loc_m = re.search(r'sr-job-item__facet-icon sr-job-location">([^<]+)', item)
        location_text = normalize_text(loc_m.group(1)) if loc_m else ""

        type_m = re.search(r'sr-job-item__facet-icon sr-job-type">([^<]+)', item)
        remote_type = normalize_text(type_m.group(1)) if type_m else ""

        jobs.append({
            "id": job_id,
            "title": title,
            "url": url,
            "location_text": location_text,
            "remote_type": remote_type,
        })
    return jobs


def parse_detail_page(html: str, job: dict) -> dict:
    """Enrich job dict with data from the detail page."""
    # JSON-LD extraction
    json_lds = re.findall(
        r'<script type="application/ld\+json">(.*?)</script>', html, re.S
    )
    locations = []
    emp_type = ""
    date_posted = ""
    description = ""
    identifier = ""

    for jld in json_lds:
        try:
            data = json.loads(jld.strip())
            if data.get("@type") == "JobPosting":
                emp_type = data.get("employmentType", "")
                date_posted = data.get("datePosted", "")
                description = BeautifulSoup(
                    data.get("description", ""), "html.parser"
                ).get_text(separator=" ")[:3000]
                identifier = str(data.get("identifier", ""))
                for loc_item in data.get("jobLocation", []):
                    addr = loc_item.get("address", {})
                    locations.append({
                        "city": addr.get("addressLocality", ""),
                        "region": addr.get("addressRegion", ""),
                        "country": addr.get("addressCountry", ""),
                    })
        except Exception:
            pass

    # Structured fields from HTML body
    jfg = _extract_field(html, "Job Family Group")
    jf = _extract_field(html, "Job Family")
    time_type = _extract_field(html, "Time Type")

    # Primary location: prefer first from JSON-LD, fallback to listing text
    if locations:
        first_loc = locations[0]
        raw_country = first_loc.get("country", "")
        city = first_loc.get("city", "")
        country_fr, region_fr = normalize_country(raw_country)
        if len(locations) > 1:
            location_str = f"Multiple Locations ({', '.join(l.get('country','') for l in locations[:3])})"
        else:
            parts = [p for p in [city, first_loc.get("region",""), raw_country] if p]
            location_str = ", ".join(parts)
    else:
        # Parse from listing text "City, Region, Country"
        lt = job.get("location_text", "")
        if lt and lt != "Multiple Locations":
            parts = [p.strip() for p in lt.split(",")]
            city = parts[0] if parts else ""
            raw_country = parts[-1] if len(parts) > 1 else ""
            country_fr, region_fr = normalize_country(raw_country)
            location_str = lt
        else:
            city = ""
            country_fr = "Non spécifié"
            region_fr = "Autres"
            location_str = job.get("location_text", "")

    # Normalize date_posted
    if date_posted:
        try:
            # Format: "2026-5-29" → "2026-05-29"
            parts = date_posted.split("-")
            date_posted = f"{parts[0]}-{int(parts[1]):02d}-{int(parts[2]):02d}"
        except Exception:
            pass

    # Map family
    family = map_family(jfg, jf, job.get("title", ""), description[:500])

    # Map experience
    career_level = job.get("career_level", "")
    experience = map_experience_level(job.get("title", ""), career_level, jf)

    # Map contract
    contract = map_contract(time_type, emp_type, job.get("title", ""))

    # Map remote type
    remote = map_remote(job.get("remote_type", ""))

    return {
        **job,
        "company": "Citi",
        "location": location_str,
        "city": city,
        "country": country_fr,
        "region": region_fr,
        "job_family": family,
        "job_family_group": jfg,
        "contract_type": contract,
        "experience_level": experience,
        "remote_type": remote,
        "date_posted": date_posted,
        "description": description,
        "scraped_at": datetime.now(timezone.utc).isoformat(),
        "status": "Live",
    }


# ─── Async fetching ────────────────────────────────────────────────────────────
async def fetch_html(
    session: aiohttp.ClientSession, url: str, retries: int = 3
) -> str:
    for attempt in range(retries):
        try:
            async with session.get(url, headers=HEADERS, ssl=False) as resp:
                if resp.status == 200:
                    return await resp.text(errors="replace")
                elif resp.status == 429:
                    wait = 10 * (attempt + 1)
                    log.warning(f"Rate limited on {url}, waiting {wait}s")
                    await asyncio.sleep(wait)
                else:
                    log.warning(f"HTTP {resp.status} for {url}")
                    return ""
        except Exception as e:
            log.warning(f"Error fetching {url}: {e} (attempt {attempt+1})")
            await asyncio.sleep(2 * (attempt + 1))
    return ""


async def fetch_listing_page(
    session: aiohttp.ClientSession, page: int
) -> list[dict]:
    """Fetch one listing page and return basic job list."""
    url = f"{SEARCH_URL}?p={page}" if page > 1 else SEARCH_URL
    html = await fetch_html(session, url)
    if not html:
        return []
    return parse_listing_page(html)


async def fetch_detail(
    session: aiohttp.ClientSession,
    semaphore: asyncio.Semaphore,
    job: dict,
    conn: sqlite3.Connection,
    lock: asyncio.Lock,
) -> None:
    """Fetch and process a single job detail page."""
    async with semaphore:
        await asyncio.sleep(DELAY_DETAIL)
        html = await fetch_html(session, job["url"])
        if not html:
            log.warning(f"Empty detail page for job {job['id']}: {job['url']}")
            return
        try:
            enriched = parse_detail_page(html, job)
            async with lock:
                upsert_job(conn, enriched)
        except Exception as e:
            log.error(f"Error processing job {job['id']}: {e}")


# ─── Main scraping logic ───────────────────────────────────────────────────────
async def scrape_all(conn: sqlite3.Connection) -> int:
    """Scrape all Citi jobs. Returns total count inserted."""
    connector = aiohttp.TCPConnector(limit=MAX_CONCURRENT, ssl=False)
    timeout = aiohttp.ClientTimeout(total=30)

    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        # Phase 1: Determine total pages
        log.info("Phase 1: Fetching first listing page to get total count...")
        first_page_html = await fetch_html(session, SEARCH_URL)
        total_results_m = re.search(r'data-total-results="(\d+)"', first_page_html)
        total_pages_m = re.search(r'data-total-pages="(\d+)"', first_page_html)

        total_results = int(total_results_m.group(1)) if total_results_m else 0
        total_pages = int(total_pages_m.group(1)) if total_pages_m else 0

        log.info(f"Total jobs: {total_results}, Total pages: {total_pages}")

        # Phase 2: Collect all job stubs from listing pages
        log.info(f"Phase 2: Collecting jobs from {total_pages} listing pages...")
        all_jobs: list[dict] = parse_listing_page(first_page_html)

        for page in range(2, total_pages + 1):
            await asyncio.sleep(DELAY_BETWEEN_PAGES)
            page_jobs = await fetch_listing_page(session, page)
            all_jobs.extend(page_jobs)
            if page % 10 == 0:
                log.info(f"  Listing pages: {page}/{total_pages}, jobs collected: {len(all_jobs)}")

        log.info(f"Phase 2 done: {len(all_jobs)} job stubs collected")

        # Deduplicate by ID
        seen = set()
        unique_jobs = []
        for j in all_jobs:
            if j["id"] not in seen:
                seen.add(j["id"])
                unique_jobs.append(j)
        log.info(f"After dedup: {len(unique_jobs)} unique jobs")

        # Phase 3: Fetch detail pages concurrently
        log.info(f"Phase 3: Fetching {len(unique_jobs)} detail pages...")
        semaphore = asyncio.Semaphore(MAX_CONCURRENT)
        lock = asyncio.Lock()

        tasks = [
            fetch_detail(session, semaphore, job, conn, lock)
            for job in unique_jobs
        ]

        done = 0
        for coro in asyncio.as_completed(tasks):
            await coro
            done += 1
            if done % 100 == 0:
                log.info(f"  Detail pages: {done}/{len(unique_jobs)}")

        conn.commit()
        log.info(f"Phase 3 done: processed {done} jobs")

        return len(unique_jobs)


def main() -> None:
    log.info("=" * 60)
    log.info("Citi Bank Scraper — démarrage")
    log.info("=" * 60)

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    init_db(conn)

    try:
        start = time.time()
        total = asyncio.run(scrape_all(conn))
        elapsed = time.time() - start

        # Stats
        cur = conn.execute("SELECT COUNT(*) FROM jobs WHERE status='Live'")
        count = cur.fetchone()[0]
        log.info(f"\n✅ Terminé en {elapsed:.0f}s — {count} offres Citi en base")

        # Family distribution
        cur = conn.execute("""
            SELECT job_family, COUNT(*) as n
            FROM jobs WHERE status='Live'
            GROUP BY job_family ORDER BY n DESC
        """)
        log.info("\n📊 Répartition par famille:")
        for row in cur.fetchall():
            log.info(f"  {row[1]:5d}  {row[0]}")

        # Country distribution
        cur = conn.execute("""
            SELECT country, COUNT(*) as n
            FROM jobs WHERE status='Live'
            GROUP BY country ORDER BY n DESC LIMIT 20
        """)
        log.info("\n🌍 Top pays:")
        for row in cur.fetchall():
            log.info(f"  {row[1]:5d}  {row[0]}")

        # Experience distribution
        cur = conn.execute("""
            SELECT experience_level, COUNT(*) as n
            FROM jobs WHERE status='Live'
            GROUP BY experience_level ORDER BY n DESC
        """)
        log.info("\n🎓 Répartition par niveau:")
        for row in cur.fetchall():
            log.info(f"  {row[1]:5d}  {row[0]}")

    finally:
        conn.close()


if __name__ == "__main__":
    main()
