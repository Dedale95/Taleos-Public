#!/usr/bin/env python3
"""
Standard Chartered — Scraper SuccessFactors CSB
=================================================
Source  : jobs.standardchartered.com (SuccessFactors Career Site Builder v3)
Phase 1 : Sitemap XML → ~1146 URLs avec date lastmod + extraction ville/titre depuis l'URL
Phase 2 : Playwright (headless) → rendu JS → extraction description + champs structurés

Le CSB SF charge tout via JavaScript ; aucune API JSON publique accessible sans auth.
L'URL slug encode ville et titre : /job/{Ville}-{Titre}/{ID}/
"""
from __future__ import annotations

import asyncio
import logging
import re
import sqlite3
import urllib.request
import xml.etree.ElementTree as ET
from datetime import datetime
from pathlib import Path
from urllib.parse import unquote
from typing import Optional

# ───────────────────────────────── Playwright ────────────────────────────────
try:
    from playwright.async_api import async_playwright, BrowserContext, Page
    PLAYWRIGHT_AVAILABLE = True
except ImportError:
    PLAYWRIGHT_AVAILABLE = False

# ───────────────────────────────── Logging ──────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(Path(__file__).parent / "standard_chartered_scraper.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger(__name__)

# ────────────────────────────────── Config ───────────────────────────────────
DB_PATH      = Path(__file__).parent / "standard_chartered_jobs.db"
SITEMAP_URL  = "https://jobs.standardchartered.com/sitemap.xml"
BASE_URL     = "https://jobs.standardchartered.com"
CONCURRENCY  = 3        # pages Playwright simultanées (SC rate-limit agressif → max 3)
PAGE_TIMEOUT = 20_000   # ms
HEADLESS     = True
USER_AGENT   = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)

# ──────────────────── Mapping Ville → (pays_fr, région) ──────────────────────
# SC opère dans 54 marchés — focus Asie/Afrique/M-O/Europe
CITY_MAP: dict[str, tuple[str, str]] = {
    # Inde (prédominant : Bangalore, Chennai, Mumbai)
    "bangalore":        ("Inde",             "Asie-Pacifique"),
    "bengaluru":        ("Inde",             "Asie-Pacifique"),
    "chennai":          ("Inde",             "Asie-Pacifique"),
    "mumbai":           ("Inde",             "Asie-Pacifique"),
    "pune":             ("Inde",             "Asie-Pacifique"),
    "hyderabad":        ("Inde",             "Asie-Pacifique"),
    "kolkata":          ("Inde",             "Asie-Pacifique"),
    "delhi":            ("Inde",             "Asie-Pacifique"),
    "gurugram":         ("Inde",             "Asie-Pacifique"),
    "noida":            ("Inde",             "Asie-Pacifique"),
    # Singapour
    "singapore":        ("Singapour",        "Asie-Pacifique"),
    "sinfong":          ("Singapour",        "Asie-Pacifique"),
    "tampines":         ("Singapour",        "Asie-Pacifique"),
    "ang":              ("Singapour",        "Asie-Pacifique"),  # Ang Mo Kio
    "jurong":           ("Singapour",        "Asie-Pacifique"),
    "bukit":            ("Singapour",        "Asie-Pacifique"),  # Bukit Timah
    # Hong Kong
    "hong":             ("Hong Kong",        "Asie-Pacifique"),  # Hong Kong (rare si slug commence par "Hong")
    "central":          ("Hong Kong",        "Asie-Pacifique"),  # Central district HK
    "kwun":             ("Hong Kong",        "Asie-Pacifique"),  # Kwun Tong
    "kowloon":          ("Hong Kong",        "Asie-Pacifique"),
    "admiralty":        ("Hong Kong",        "Asie-Pacifique"),
    "wanchai":          ("Hong Kong",        "Asie-Pacifique"),
    # Chine (villes principales + villes SC fréquentes)
    "shanghai":         ("Chine",            "Asie-Pacifique"),
    "beijing":          ("Chine",            "Asie-Pacifique"),
    "tianjin":          ("Chine",            "Asie-Pacifique"),
    "guangzhou":        ("Chine",            "Asie-Pacifique"),
    "shenzhen":         ("Chine",            "Asie-Pacifique"),
    "chengdu":          ("Chine",            "Asie-Pacifique"),
    "chongqing":        ("Chine",            "Asie-Pacifique"),
    "nanjing":          ("Chine",            "Asie-Pacifique"),
    "wuhan":            ("Chine",            "Asie-Pacifique"),
    "hangzhou":         ("Chine",            "Asie-Pacifique"),
    "suzhou":           ("Chine",            "Asie-Pacifique"),
    "xiamen":           ("Chine",            "Asie-Pacifique"),
    "qingdao":          ("Chine",            "Asie-Pacifique"),
    "nanshan":          ("Chine",            "Asie-Pacifique"),  # district Shenzhen
    "chengdu":          ("Chine",            "Asie-Pacifique"),
    "foshan":           ("Chine",            "Asie-Pacifique"),
    "dongguan":         ("Chine",            "Asie-Pacifique"),
    "ningbo":           ("Chine",            "Asie-Pacifique"),
    "dalian":           ("Chine",            "Asie-Pacifique"),
    # Taïwan
    "taipei":           ("Taïwan",           "Asie-Pacifique"),
    "taichung":         ("Taïwan",           "Asie-Pacifique"),
    "jhongli":          ("Taïwan",           "Asie-Pacifique"),
    "lujhu":            ("Taïwan",           "Asie-Pacifique"),
    "yangmei":          ("Taïwan",           "Asie-Pacifique"),
    "banqiao":          ("Taïwan",           "Asie-Pacifique"),
    "taoyuan":          ("Taïwan",           "Asie-Pacifique"),
    "hsinchu":          ("Taïwan",           "Asie-Pacifique"),
    "sinfong":          ("Singapour",        "Asie-Pacifique"),
    # Malaisie
    "kuala":            ("Malaisie",         "Asie-Pacifique"),  # Kuala Lumpur
    "petaling":         ("Malaisie",         "Asie-Pacifique"),  # Petaling Jaya
    # Corée
    "seoul":            ("Corée du Sud",     "Asie-Pacifique"),
    # Japon
    "tokyo":            ("Japon",            "Asie-Pacifique"),
    "osaka":            ("Japon",            "Asie-Pacifique"),
    # Vietnam
    "ho":               ("Vietnam",          "Asie-Pacifique"),  # Ho Chi Minh
    "hanoi":            ("Vietnam",          "Asie-Pacifique"),
    # Philippines
    "manila":           ("Philippines",      "Asie-Pacifique"),
    "taguig":           ("Philippines",      "Asie-Pacifique"),
    "makati":           ("Philippines",      "Asie-Pacifique"),
    # Indonésie
    "jakarta":          ("Indonésie",        "Asie-Pacifique"),
    # Thaïlande
    "bangkok":          ("Thaïlande",        "Asie-Pacifique"),
    # Pakistan
    "karachi":          ("Pakistan",         "Asie-Pacifique"),
    # Bangladesh
    "dhaka":            ("Bangladesh",       "Asie-Pacifique"),
    "chittagong":       ("Bangladesh",       "Asie-Pacifique"),
    # Sri Lanka
    "colombo":          ("Sri Lanka",        "Asie-Pacifique"),
    # UK
    "london":           ("Royaume-Uni",      "Europe"),
    "edinburgh":        ("Royaume-Uni",      "Europe"),
    "glasgow":          ("Royaume-Uni",      "Europe"),
    "birmingham":       ("Royaume-Uni",      "Europe"),
    "newcastle":        ("Royaume-Uni",      "Europe"),
    # Europe
    "paris":            ("France",           "Europe"),
    "frankfurt":        ("Allemagne",        "Europe"),
    "warsaw":           ("Pologne",          "Europe"),
    "warszawa":         ("Pologne",          "Europe"),
    "amsterdam":        ("Pays-Bas",         "Europe"),
    "milan":            ("Italie",           "Europe"),
    "madrid":           ("Espagne",          "Europe"),
    "luxemburg":        ("Luxembourg",       "Europe"),
    "luxembourg":       ("Luxembourg",       "Europe"),
    "zurich":           ("Suisse",           "Europe"),
    "budapest":         ("Hongrie",          "Europe"),
    # MENA
    "dubai":            ("Émirats arabes unis", "Moyen-Orient / Afrique"),
    "abu":              ("Émirats arabes unis", "Moyen-Orient / Afrique"),  # Abu Dhabi
    "doha":             ("Qatar",            "Moyen-Orient / Afrique"),
    "riyadh":           ("Arabie saoudite",  "Moyen-Orient / Afrique"),
    "manama":           ("Bahreïn",          "Moyen-Orient / Afrique"),
    "cairo":            ("Égypte",           "Moyen-Orient / Afrique"),
    "lagos":            ("Nigeria",          "Moyen-Orient / Afrique"),
    "nairobi":          ("Kenya",            "Moyen-Orient / Afrique"),
    "johannesburg":     ("Afrique du Sud",   "Moyen-Orient / Afrique"),
    "ghana":            ("Ghana",            "Moyen-Orient / Afrique"),
    "accra":            ("Ghana",            "Moyen-Orient / Afrique"),
    "dar":              ("Tanzanie",         "Moyen-Orient / Afrique"),  # Dar es Salaam
    "khartoum":         ("Soudan",           "Moyen-Orient / Afrique"),
    "kampala":          ("Ouganda",          "Moyen-Orient / Afrique"),
    "lusaka":           ("Zambie",           "Moyen-Orient / Afrique"),
    "harare":           ("Zimbabwe",         "Moyen-Orient / Afrique"),
    # Amériques
    "new":              ("États-Unis",       "Amérique du Nord"),  # New York
    "sao":              ("Brésil",           "Amérique du Sud"),
    "bogota":           ("Colombie",         "Amérique du Sud"),
    "houston":          ("États-Unis",       "Amérique du Nord"),
    "chicago":          ("États-Unis",       "Amérique du Nord"),
    "miami":            ("États-Unis",       "Amérique du Nord"),
    "atlanta":          ("États-Unis",       "Amérique du Nord"),
    "boston":           ("États-Unis",       "Amérique du Nord"),
    "washington":       ("États-Unis",       "Amérique du Nord"),
    "toronto":          ("Canada",           "Amérique du Nord"),
    "montreal":         ("Canada",           "Amérique du Nord"),
    "lima":             ("Pérou",            "Amérique du Sud"),
    "santiago":         ("Chili",            "Amérique du Sud"),
    "buenos":           ("Argentine",        "Amérique du Sud"),  # Buenos Aires
    # Autres
    "multan":           ("Pakistan",         "Asie-Pacifique"),
    "lahore":           ("Pakistan",         "Asie-Pacifique"),
    "kathmandu":        ("Népal",            "Asie-Pacifique"),
    "victoria":         ("Nigeria",          "Moyen-Orient / Afrique"),  # Victoria Island Lagos
    "brunei":           ("Brunéi",           "Asie-Pacifique"),
    "georgetown":       ("Malaisie",         "Asie-Pacifique"),  # Georgetown Penang
    "yangon":           ("Myanmar",          "Asie-Pacifique"),
    "phnom":            ("Cambodge",         "Asie-Pacifique"),  # Phnom Penh
    "vientiane":        ("Laos",             "Asie-Pacifique"),
    # Australie
    "sydney":           ("Australie",        "Asie-Pacifique"),
    "melbourne":        ("Australie",        "Asie-Pacifique"),
}

# Surcharges pour certains slugs ambigus
SLUG_OVERRIDES: dict[str, tuple[str, str, str]] = {
    # slug_start → (city_display, country_fr, region)
    "kwun-tong":      ("Kwun Tong",       "Hong Kong",            "Asie-Pacifique"),
    "quarry-bay":     ("Quarry Bay",      "Hong Kong",            "Asie-Pacifique"),
    "wan-chai":       ("Wan Chai",        "Hong Kong",            "Asie-Pacifique"),
    "new-york":       ("New York",        "États-Unis",           "Amérique du Nord"),
    "san-francisco":  ("San Francisco",   "États-Unis",           "Amérique du Nord"),
    "los-angeles":    ("Los Angeles",     "États-Unis",           "Amérique du Nord"),
    "ho-chi":         ("Ho Chi Minh",     "Vietnam",              "Asie-Pacifique"),
    "kuala-lumpur":   ("Kuala Lumpur",    "Malaisie",             "Asie-Pacifique"),
    "petaling-jaya":  ("Petaling Jaya",   "Malaisie",             "Asie-Pacifique"),
    "kota-kinabalu":  ("Kota Kinabalu",   "Malaisie",             "Asie-Pacifique"),
    "sao-paulo":      ("São Paulo",       "Brésil",               "Amérique du Sud"),
    "abu-dhabi":      ("Abu Dhabi",       "Émirats arabes unis",  "Moyen-Orient / Afrique"),
    "bukit-timah":    ("Bukit Timah",     "Singapour",            "Asie-Pacifique"),
    "ang-mo-kio":     ("Ang Mo Kio",      "Singapour",            "Asie-Pacifique"),
    "taichung-city":  ("Taichung City",   "Taïwan",               "Asie-Pacifique"),
    "victoria-island":("Victoria Island", "Nigeria",              "Moyen-Orient / Afrique"),
    "dar-es-salaam":  ("Dar es Salaam",   "Tanzanie",             "Moyen-Orient / Afrique"),
    "buenos-aires":   ("Buenos Aires",    "Argentine",            "Amérique du Sud"),
    "phnom-penh":     ("Phnom Penh",      "Cambodge",             "Asie-Pacifique"),
}

# ─────────────────────── Classification niveau d'expérience ─────────────────
LEVEL_PATTERNS: list[tuple[re.Pattern, str]] = [
    (re.compile(r"\b(managing director|md)\b",              re.I), "11 ans et plus"),
    (re.compile(r"\b(executive director|ed)\b",              re.I), "11 ans et plus"),
    (re.compile(r"\b(director)\b",                           re.I), "11 ans et plus"),
    (re.compile(r"\b(senior vice president|svp)\b",          re.I), "11 ans et plus"),
    (re.compile(r"\b(vice president|vp)\b",                  re.I), "6 - 10 ans"),
    (re.compile(r"\b(senior associate director|sad)\b",      re.I), "6 - 10 ans"),
    (re.compile(r"\b(associate director|ad)\b",              re.I), "6 - 10 ans"),
    (re.compile(r"\b(senior manager|head of)\b",             re.I), "6 - 10 ans"),
    (re.compile(r"\b(manager)\b",                            re.I), "3 - 5 ans"),
    (re.compile(r"\b(senior|sr\.)\b",                        re.I), "3 - 5 ans"),
    (re.compile(r"\b(associate)\b",                          re.I), "3 - 5 ans"),
    (re.compile(r"\b(analyst|officer|specialist)\b",         re.I), "0 - 2 ans"),
    (re.compile(r"\b(intern|internship|graduate|trainee|apprentice)\b", re.I), "0 - 2 ans"),
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
            company_name        TEXT DEFAULT 'Standard Chartered',
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


# ──────────────────────────── Helpers localisation ───────────────────────────
def _extract_city_title_from_slug(slug: str) -> tuple[str, str, str, str]:
    """
    Extrait (city_display, title, country_fr, region) depuis le slug URL SC.
    Format slug : {City[-City2]}-{Title},{SubTitle}/{ID}

    Stratégie :
    1. Test surcharges explicites (kwun-tong, new-york…)
    2. Mappage du premier token sur CITY_MAP
    3. Fallback : premier token = ville, reste = titre
    """
    decoded = unquote(slug).replace("&amp;", "&")
    slug_lower = slug.lower()

    # 1. Surcharges
    for key, (city, country, region) in sorted(SLUG_OVERRIDES.items(), key=lambda x: -len(x[0])):
        if slug_lower.startswith(key):
            rest = decoded[len(key):].lstrip("-,").strip()
            title = re.sub(r"-(?![0-9])", " ", rest)
            title = re.sub(r",\s*", ", ", title).strip()
            title = re.sub(r"\s+[A-Z]{2}-\d+$", "", title).strip()
            title = re.sub(r"\s+\d+$", "", title).strip()
            return city, title, country, region

    # 2. Identifier la ville depuis le DÉBUT du slug (1 ou 2 tokens)
    #    RÈGLE : la ville est TOUJOURS au début du slug, jamais après un tiret intermédiaire.
    #    ⚠️  NE PAS utiliser la première virgule comme séparateur ville/titre :
    #         "Suzhou-Senior-Relationship-Manager,-Priority" → virgule après le titre !
    parts = decoded.split("-")

    # Essai 2 tokens (ex: "Taichung City", "Quarry Bay" non dans SLUG_OVERRIDES)
    if len(parts) >= 2:
        two_word_key = f"{parts[0]}-{parts[1]}".lower()
        if two_word_key in CITY_MAP:
            country_fr, region = CITY_MAP[two_word_key]
            city_display = f"{parts[0]} {parts[1]}"
            rest_parts = parts[2:]
        else:
            # Essai 1 token
            first = parts[0].lower()
            country_fr, region = CITY_MAP.get(first, ("", ""))
            if country_fr:
                city_display = parts[0]
                rest_parts = parts[1:]
            else:
                # Ville inconnue → utiliser seulement le PREMIER token comme ville
                # (ne jamais prendre tout avant la virgule, ça absorbe le titre !)
                city_display = parts[0]
                rest_parts = parts[1:]
                country_fr, region = "", "Autres"
    else:
        city_display = decoded
        rest_parts = []
        country_fr, region = "", "Autres"

    title_raw = "-".join(rest_parts)
    # Nettoyer le titre : remplacer les tirets entre mots par espaces sauf avant chiffres
    title = re.sub(r"-(?![0-9])", " ", title_raw)
    title = re.sub(r",\s*", ", ", title).strip()
    # Supprimer les suffixes numériques et codes postaux en fin de titre
    title = re.sub(r"\s+[A-Z]{2}-\d+$", "", title).strip()  # ex: "TX-77001"
    title = re.sub(r"\s+\d+$", "", title).strip()

    return city_display, title, country_fr or "Non spécifié", region or "Autres"


# ──────────────────────────── Phase 1 : sitemap ──────────────────────────────
def fetch_sitemap() -> list[dict]:
    """Parse le sitemap XML et retourne la liste des jobs avec URL + date."""
    logger.info(f"Phase 1 — Fetch sitemap: {SITEMAP_URL}")
    req = urllib.request.Request(SITEMAP_URL, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=30) as r:
        xml_content = r.read()

    root = ET.fromstring(xml_content)
    ns = {"sm": "http://www.google.com/schemas/sitemap/0.9"}

    jobs = []
    for url_el in root.findall(".//sm:url", ns):
        loc = url_el.findtext("sm:loc", namespaces=ns) or ""
        lastmod = url_el.findtext("sm:lastmod", namespaces=ns) or ""

        if "/job/" not in loc:
            continue

        # Extrait le chemin /job/Slug/ID/
        path_match = re.search(r"/job/([^/]+)/(\d+)/?$", loc)
        if not path_match:
            continue

        slug_raw, job_id = path_match.group(1), path_match.group(2)
        city, title, country_fr, region = _extract_city_title_from_slug(slug_raw)

        # Date (lastmod → YYYY-MM-DD)
        pub_date = lastmod[:10] if lastmod else datetime.utcnow().strftime("%Y-%m-%d")

        # Type de contrat depuis le titre
        title_l = title.lower()
        if any(k in title_l for k in ["intern", "internship", "graduate", "trainee", "apprentice"]):
            contract = "Stage"
        else:
            contract = "CDI"

        location = f"{city} - {country_fr}" if city and country_fr and country_fr != "Non spécifié" else (city or country_fr or "")

        jobs.append({
            "job_url":          loc,
            "job_id":           job_id,
            "job_title":        title,
            "contract_type":    contract,
            "publication_date": pub_date,
            "location":         location,
            "country":          country_fr,
            "region":           region,
            "job_family":       "",
            "experience_level": _classify_level(title),
            "education_level":  "",
            "job_description":  "",
            "company_name":     "Standard Chartered",
            "status":           "Live",
        })

    logger.info(f"Phase 1 terminée — {len(jobs)} offres dans le sitemap")
    return jobs


# ──────────────────────────── Phase 2 : Playwright ───────────────────────────
async def _navigate(page: Page, url: str, retries: int = 3) -> None:
    for attempt in range(retries):
        try:
            await page.goto(url, timeout=PAGE_TIMEOUT, wait_until="domcontentloaded")
            return
        except Exception as exc:
            if attempt == retries - 1:
                raise
            await asyncio.sleep(2 ** attempt)


async def _scrape_detail(context: BrowserContext, url: str,
                          sem: asyncio.Semaphore) -> tuple[str, str]:
    """Retourne (job_url, description_text)."""
    async with sem:
        page = await context.new_page()
        try:
            await page.route(
                "**/*",
                lambda route: route.abort()
                if route.request.resource_type in ("image", "media", "font", "stylesheet")
                else route.continue_(),
            )
            await asyncio.sleep(0.8)  # throttle anti-429 avant la requête
            await _navigate(page, url)
            # Attendre que le JS charge le contenu (SC CSB met ~2-3s)
            await asyncio.sleep(4.0)
            html = await page.content()

            # Vérifier si l'offre est expirée
            page_text = await page.inner_text("body")
            if any(s in page_text.lower() for s in [
                "this job is no longer available",
                "this position is no longer available",
                "job posting has expired",
            ]):
                return url, "__EXPIRED__"

            # Extraction du texte principal
            desc = ""
            # Cherche le bloc de description dans le HTML rendu
            desc_match = re.search(
                r'<div[^>]+(?:class|id)="[^"]*(?:job-desc|description|content|mainContent)[^"]*"[^>]*>(.*?)</div>',
                html, re.DOTALL | re.I
            )
            if desc_match:
                desc = re.sub(r"<[^>]+>", " ", desc_match.group(1))
            else:
                # Fallback : texte complet de la page
                desc = page_text

            desc = re.sub(r"\s+", " ", desc).strip()[:25_000]
            return url, desc

        except Exception as exc:
            logger.warning(f"Detail failed: {url} — {exc}")
            return url, ""
        finally:
            await page.close()


async def scrape_descriptions(jobs: list[dict], conn: sqlite3.Connection) -> list[dict]:
    """Enrichit les jobs avec les descriptions via Playwright."""
    if not PLAYWRIGHT_AVAILABLE:
        logger.warning("Playwright non disponible — descriptions ignorées")
        return jobs

    logger.info(f"Phase 2 — Playwright scraping ({len(jobs)} offres, concurrency={CONCURRENCY})…")
    sem = asyncio.Semaphore(CONCURRENCY)
    url_to_desc: dict[str, str] = {}
    expired_urls: set[str] = set()
    done = 0

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=HEADLESS)
        context = await browser.new_context(user_agent=USER_AGENT)
        try:
            tasks = [_scrape_detail(context, j["job_url"], sem) for j in jobs]
            for coro in asyncio.as_completed(tasks):
                job_url, desc = await coro
                if desc == "__EXPIRED__":
                    expired_urls.add(job_url)
                else:
                    url_to_desc[job_url] = desc
                done += 1
                if done % 100 == 0:
                    logger.info(f"  Phase 2 : {done}/{len(jobs)}")
        finally:
            await browser.close()

    if expired_urls:
        logger.info(f"  → {len(expired_urls)} offres expirées détectées")
        mark_expired(conn, expired_urls)

    # Enrichir les jobs avec les descriptions
    for job in jobs:
        if job["job_url"] in url_to_desc:
            job["job_description"] = url_to_desc[job["job_url"]]
        if job["job_url"] in expired_urls:
            job["status"] = "Expired"

    logger.info(f"Phase 2 terminée — {done} pages traitées, {len(expired_urls)} expirées")
    return jobs


# ────────────────────────────────── Main ─────────────────────────────────────
async def main() -> None:
    logger.info("=== Standard Chartered Scraper ===")
    conn = init_db(DB_PATH)
    existing_urls = get_existing_urls(conn)
    logger.info(f"DB existante : {len(existing_urls)} offres")

    # Phase 1 : sitemap
    jobs = fetch_sitemap()
    if not jobs:
        logger.error("Sitemap vide — abandon")
        conn.close()
        return

    api_urls = {j["job_url"] for j in jobs}
    newly_expired = existing_urls - api_urls
    if newly_expired:
        logger.info(f"  → {len(newly_expired)} offres disparues du sitemap → Expired")
        mark_expired(conn, newly_expired)

    # Phase 2 : descriptions via Playwright
    jobs = await scrape_descriptions(jobs, conn)

    # Sauvegarder uniquement les offres Live
    live_jobs = [j for j in jobs if j.get("status") == "Live"]
    saved = upsert_jobs(conn, live_jobs)
    total_live = conn.execute(
        "SELECT COUNT(*) FROM jobs WHERE is_valid=1 AND status='Live'"
    ).fetchone()[0]
    conn.close()
    logger.info(f"✅ Standard Chartered — {saved} offres upsert, {total_live} offres Live en base")


if __name__ == "__main__":
    asyncio.run(main())
