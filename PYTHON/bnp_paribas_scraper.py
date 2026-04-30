#!/usr/bin/env python3
"""
BNP PARIBAS - JOB SCRAPER
Pipeline optimisé pour scraper les offres d'emploi du groupe BNP Paribas.
Utilise Playwright (async) + BeautifulSoup pour contourner les protections anti-bot.

Robustesse :
  - Détection fine des crashes Playwright (EPIPE, "canceled", navigateur tué)
  - navigate_with_retry ne réessaie PAS sur une erreur de crash (retour immédiat)
  - Boucle de redémarrage navigateur : si un crash survient dans la phase détail,
    le navigateur est relancé et le scraping reprend depuis la DB (offres déjà
    sauvegardées ignorées → aucune perte de données)
  - Sauvegarde immédiate en DB après chaque offre (commit unitaire)
  - Séparation listing / détail : deux contextes navigateur indépendants
"""

import asyncio
import logging
import os
import re
import time
import sqlite3
import json
import random
import pandas as pd
from pathlib import Path
from typing import List, Dict, Set, Optional, Tuple
from playwright.async_api import async_playwright, BrowserContext, Browser, Page
from bs4 import BeautifulSoup
from tqdm.asyncio import tqdm
from datetime import datetime

try:
    from city_normalizer import normalize_city
    from country_normalizer import normalize_country, get_country_from_city
    from job_family_classifier import classify_job_family
    from experience_extractor import extract_experience_level
except ImportError:
    import sys
    sys.path.append(str(Path(__file__).parent))
    from city_normalizer import normalize_city
    from country_normalizer import normalize_country, get_country_from_city
    from job_family_classifier import classify_job_family
    from experience_extractor import extract_experience_level

# ================= Logging =================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# ================= Constants =================
BASE_URL = "https://group.bnpparibas"
SEARCH_URL = f"{BASE_URL}/emploi-carriere/toutes-offres-emploi"

# Filtres par type de contrat : le site BNP expose une URL par type.
# Ordre : types génériques d'abord, puis spécifiques (Graduate Programme, Job étudiant)
# pour que les doublons gardent le type le plus précis.
CONTRACT_FILTERS = [
    ("cdi", "CDI"),
    ("cdd", "CDD"),
    ("stage", "Stage"),
    ("vie", "VIE"),
    ("job-etudiant", "Job étudiant"),
    ("alternance", "Alternance / Apprentissage"),
    ("graduate-programme-cdi", "Graduate Programme (CDI)"),
    ("zero-hours", "Zero Hours"),
]

# ================= Config =================
class Config:
    MAX_CONCURRENT_LISTING = 8    # Pages de liste (réduit pour stabilité)
    MAX_CONCURRENT_DETAILS = 5    # Pages détail (réduit pour éviter TargetClosedError/Timeout)
    PAGE_TIMEOUT = 30000          # 30 s — suffisant pour les pages BNP ; libère vite les slots sur timeout
    WAIT_TIMEOUT = 10000
    HEADLESS = True
    BASE_DIR = Path(__file__).parent
    DB_PATH = BASE_DIR / "bnp_paribas_jobs.db"
    CSV_PATH = BASE_DIR / "bnp_paribas_jobs.csv"
    # Nombre maximum de redémarrages du navigateur sur crash Playwright
    MAX_BROWSER_RESTARTS = 5

    BLOCK_RESOURCES = {
        "image", "font", "media", "texttrack",
        "object", "beacon", "csp_report", "imageset"
    }

    USER_AGENTS = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:123.0) Gecko/20100101 Firefox/123.0",
    ]

config = Config()

# ================= Contract type mapping =================
CONTRACT_MAPPING = {
    "cdi": "CDI",
    "cdi (permanent)": "CDI",
    "permanent": "CDI",
    "cdd": "CDD",
    "cdd (temporary)": "CDD",
    "temporary": "CDD",
    "stage": "Stage",
    "internship": "Stage",
    "vie": "VIE",
    "v.i.e": "VIE",
    "v.i.e.": "VIE",
    "alternance": "Alternance / Apprentissage",
    "apprenticeship": "Alternance / Apprentissage",
    "job étudiant": "Job étudiant",
    "student job": "Job étudiant",
    "graduate programme (cdi)": "CDI",  # contrat sous-jacent = CDI
    "graduate program": "Graduate Programme",
    "zero hours": "Zero Hours",
}


def normalize_contract_type(raw: str) -> Optional[str]:
    if not raw:
        return None
    cleaned = raw.strip().lower()
    if cleaned in CONTRACT_MAPPING:
        return CONTRACT_MAPPING[cleaned]
    cleaned_no_paren = re.sub(r'\s*\(.*?\)\s*', '', cleaned).strip()
    if cleaned_no_paren in CONTRACT_MAPPING:
        return CONTRACT_MAPPING[cleaned_no_paren]
    return raw.strip()


# =========================================================
# DATABASE MANAGER
# =========================================================
class JobDatabase:
    """Gestion de la base de données SQLite pour BNP Paribas"""

    def __init__(self, db_path: Path):
        self.db_path = db_path
        self.init_db()

    def init_db(self):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS jobs (
                    job_url TEXT PRIMARY KEY,
                    job_id TEXT,
                    job_title TEXT,
                    contract_type TEXT,
                    publication_date TEXT,
                    location TEXT,
                    job_family TEXT,
                    duration TEXT,
                    management_position TEXT,
                    status TEXT DEFAULT 'Live',
                    education_level TEXT,
                    experience_level TEXT,
                    training_specialization TEXT,
                    technical_skills TEXT,
                    behavioral_skills TEXT,
                    tools TEXT,
                    languages TEXT,
                    job_description TEXT,
                    company_name TEXT,
                    company_description TEXT,
                    first_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    scrape_attempts INTEGER DEFAULT 0,
                    is_valid INTEGER DEFAULT 1
                )
            """)
            conn.commit()

    def get_live_urls(self) -> Set[str]:
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                "SELECT job_url FROM jobs WHERE status = 'Live' AND is_valid = 1"
            )
            return {row[0] for row in cursor.fetchall()}

    def get_jobs_in_db(self, urls: Set[str]) -> Set[str]:
        """Retourne le sous-ensemble d'URLs déjà présentes en base (peu importe le statut).
        Utilisé après un crash pour éviter de re-scraper les offres déjà sauvegardées."""
        if not urls:
            return set()
        with sqlite3.connect(self.db_path) as conn:
            placeholders = ','.join('?' * len(urls))
            cursor = conn.execute(
                f"SELECT job_url FROM jobs WHERE job_url IN ({placeholders})",
                tuple(urls)
            )
            return {row[0] for row in cursor.fetchall()}

    def get_existing_publication_date(self, job_url: str) -> Optional[str]:
        with sqlite3.connect(self.db_path) as conn:
            row = conn.execute(
                "SELECT publication_date, first_seen FROM jobs WHERE job_url = ?",
                (job_url,)
            ).fetchone()
            if not row:
                return None
            pub_date, first_seen = row[0], row[1]
            if pub_date and str(pub_date).strip():
                return pub_date
            if first_seen:
                return str(first_seen).strip()[:10]
            return None

    def mark_as_expired(self, urls: Set[str]):
        if not urls:
            return
        with sqlite3.connect(self.db_path) as conn:
            placeholders = ','.join('?' * len(urls))
            conn.execute(f"""
                UPDATE jobs
                SET status = 'Expired', last_updated = CURRENT_TIMESTAMP
                WHERE job_url IN ({placeholders})
            """, tuple(urls))
            conn.commit()

    def insert_or_update_job(self, job: Dict):
        with sqlite3.connect(self.db_path) as conn:
            is_valid = 1 if (
                job.get('job_id') or job.get('job_title') or job.get('job_description')
            ) else 0

            technical_skills = job.get('technical_skills', '[]')
            behavioral_skills = job.get('behavioral_skills', '[]')

            if isinstance(technical_skills, list):
                technical_skills = json.dumps(technical_skills, ensure_ascii=False)
            elif not isinstance(technical_skills, str) or not technical_skills.startswith('['):
                technical_skills = json.dumps([], ensure_ascii=False)

            if isinstance(behavioral_skills, list):
                behavioral_skills = json.dumps(behavioral_skills, ensure_ascii=False)
            elif not isinstance(behavioral_skills, str) or not behavioral_skills.startswith('['):
                behavioral_skills = json.dumps([], ensure_ascii=False)

            conn.execute("""
                INSERT INTO jobs (
                    job_url, job_id, job_title, contract_type, publication_date,
                    location, job_family, duration, management_position, status,
                    education_level, experience_level, training_specialization,
                    technical_skills, behavioral_skills, tools, languages,
                    job_description, company_name, company_description,
                    scrape_attempts, is_valid, last_updated
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, CURRENT_TIMESTAMP)
                ON CONFLICT(job_url) DO UPDATE SET
                    job_id = excluded.job_id,
                    job_title = excluded.job_title,
                    contract_type = excluded.contract_type,
                    publication_date = COALESCE(
                        NULLIF(TRIM(COALESCE(excluded.publication_date,'')), ''),
                        jobs.publication_date
                    ),
                    location = excluded.location,
                    job_family = excluded.job_family,
                    duration = excluded.duration,
                    management_position = excluded.management_position,
                    status = excluded.status,
                    education_level = excluded.education_level,
                    experience_level = excluded.experience_level,
                    training_specialization = excluded.training_specialization,
                    technical_skills = excluded.technical_skills,
                    behavioral_skills = excluded.behavioral_skills,
                    tools = excluded.tools,
                    languages = excluded.languages,
                    job_description = excluded.job_description,
                    company_name = excluded.company_name,
                    company_description = excluded.company_description,
                    scrape_attempts = scrape_attempts + 1,
                    is_valid = excluded.is_valid,
                    last_updated = CURRENT_TIMESTAMP
            """, (
                job.get('job_url'), job.get('job_id'), job.get('job_title'),
                job.get('contract_type'), job.get('publication_date'),
                job.get('location'), job.get('job_family'), job.get('duration'),
                job.get('management_position'), job.get('status', 'Live'),
                job.get('education_level'), job.get('experience_level'),
                job.get('training_specialization'), technical_skills,
                behavioral_skills, job.get('tools'),
                job.get('languages'), job.get('job_description'),
                job.get('company_name'), job.get('company_description'), is_valid
            ))
            conn.commit()

    def export_to_csv(self, csv_path: Path):
        with sqlite3.connect(self.db_path) as conn:
            query = """
                SELECT
                    job_id, job_title, contract_type, publication_date, location,
                    job_family, duration, management_position, status,
                    education_level, experience_level, training_specialization,
                    technical_skills, behavioral_skills, tools, languages,
                    job_description, company_name, company_description, job_url,
                    first_seen, last_updated
                FROM jobs
                WHERE is_valid = 1
                ORDER BY last_updated DESC
            """
            df = pd.read_sql_query(query, conn)
            for col in ['technical_skills', 'behavioral_skills']:
                if col in df.columns:
                    df[col] = df[col].apply(
                        lambda x: ', '.join(json.loads(x))
                        if x and x.startswith('[') else x
                    )
            df.to_csv(csv_path, index=False, encoding='utf-8')


# =========================================================
# NORMALIZE LOCATION
# =========================================================
def normalize_location(location_raw: str) -> Optional[str]:
    """Normalise 'Ville, Région, Pays' → 'Ville - Pays'."""
    if not location_raw:
        return None

    parts = [p.strip() for p in location_raw.split(',')]
    city_raw = parts[0] if parts else None
    country_raw = parts[-1] if len(parts) >= 2 else None

    if not country_raw and city_raw:
        country_raw = city_raw
        city_raw = None

    known_countries = {
        'france', 'inde', 'india', 'japon', 'japan', 'pologne', 'poland',
        'roumanie', 'romania', 'chine', 'china', 'italie', 'italy',
        'allemagne', 'germany', 'espagne', 'spain', 'portugal',
        'belgique', 'belgium', 'suisse', 'switzerland', 'luxembourg',
        'pays-bas', 'netherlands', 'royaume-uni', 'united kingdom',
        'états-unis', 'united states', 'usa', 'canada', 'singapour',
        'singapore', 'hong kong', 'australie', 'australia',
        'grèce', 'greece', 'turquie', 'turkey', 'maroc', 'morocco',
        'tunisie', 'tunisia', 'algerie', 'algérie', 'algeria',
        'brésil', 'brazil', 'mexique', 'mexico', 'colombie', 'colombia',
        'argentine', 'argentina', 'chili', 'chile', 'pérou', 'peru',
        'ukraine', 'irlande', 'ireland', 'autriche', 'austria',
        'république tchèque', 'czech republic', 'hongrie', 'hungary',
        'bulgarie', 'bulgaria', 'sénégal', 'senegal',
        "côte d'ivoire", 'ivory coast', 'cameroun', 'cameroon',
        'afrique du sud', 'south africa', 'kenya', 'nigeria',
        'nouvelle-calédonie', 'la réunion', 'guyane', 'guadeloupe',
        'martinique', 'polynésie française',
    }

    if city_raw and city_raw.lower() in known_countries:
        if not country_raw:
            country_raw = city_raw
        city_raw = None

    city = normalize_city(city_raw) if city_raw else None
    country = normalize_country(country_raw) if country_raw else None

    if city and country and city.lower() == country.lower():
        return country
    elif city and country:
        return f"{city} - {country}"
    elif country:
        return country
    elif city:
        country_from_city = get_country_from_city(city)
        if country_from_city:
            return f"{city} - {normalize_country(country_from_city)}"
        return city
    return None


# =========================================================
# NORMALIZE BNP BRAND NAME
# =========================================================
BNP_BRAND_NORMALIZATION = {
    "arval bnp paribas group": "Arval",
    "arval": "Arval",
    "bnp paribas cardif": "BNP Paribas Cardif",
    "bnp paribas real estate": "BNP Paribas Real Estate",
    "bnp paribas asset management": "BNP Paribas Asset Management",
    "bnp paribas wealth management": "BNP Paribas Wealth Management",
    "bnp paribas corporate & institutional banking": "BNP Paribas Corporate & Institutional Banking",
    "bnp paribas cib": "BNP Paribas Corporate & Institutional Banking",
    "bnp paribas personal finance": "BNP Paribas Personal Finance",
    "bnp paribas personal investors": "BNP Paribas Personal Investors",
    "bnp paribas leasing solutions": "BNP Paribas Leasing Solutions",
    "bnp paribas factor": "BNP Paribas Factor",
    "bnp paribas fortis": "BNP Paribas Fortis",
    "bnp paribas bank polska": "BNP Paribas Bank Polska",
    "bgl bnp paribas": "BGL BNP Paribas",
    "bnl": "BNL",
    "teb": "TEB",
    "hello bank": "Hello bank!",
    "hello bank!": "Hello bank!",
    "banque commerciale en france": "Banque Commerciale en France",
    "bcf": "Banque Commerciale en France",
    "nickel": "Nickel",
    "alfred berg": "Alfred Berg",
}


def normalize_bnp_brand(raw: str) -> str:
    """Normalise le nom de marque BNP pour affichage cohérent."""
    if not raw or not raw.strip():
        return "BNP Paribas"
    t = raw.strip()
    t = re.sub(r'\s*\([Gg]roupe\s+BNP\s+Paribas\)\s*$', '', t).strip()
    key = t.lower()
    return BNP_BRAND_NORMALIZATION.get(key, t)


# =========================================================
# EXTRACT DETAIL PAGE METADATA FIELD
# =========================================================
def extract_offer_field(soup: BeautifulSoup, css_class: str) -> Optional[str]:
    section = soup.select_one(f"div.{css_class}")
    if not section:
        return None
    title_cat = section.select_one(".title-cat")
    if not title_cat:
        return None
    next_el = title_cat.find_next_sibling()
    if next_el:
        return next_el.get_text(strip=True) or None
    return None


# =========================================================
# PLAYWRIGHT CRASH DETECTION & BROWSER HELPERS
# =========================================================
# Marqueurs indiquant que le process Node.js de Playwright est mort.
# Dans ce cas, réessayer sur la même page/contexte est inutile :
# il faut relancer un navigateur complet.
_BROWSER_CRASH_MARKERS = frozenset({
    "write epipe",
    "the operation was canceled",
    "browser has been closed",
    "browser is closed",
    "target closed",
    "connection closed",
    "browser context closed",
    "playwright: browser closed",
})


def _is_browser_crash(exc: Exception) -> bool:
    """Détecte une panne Playwright (EPIPE Node.js, navigateur tué, contexte fermé).
    Un crash n'est PAS récupérable par un simple retry : il faut relancer le navigateur."""
    msg = str(exc).lower()
    return any(marker in msg for marker in _BROWSER_CRASH_MARKERS)


class BrowserCrashError(Exception):
    """Levée quand le process Playwright est mort (EPIPE, navigateur tué…).
    Propagée jusqu'à scrape_details_robust qui redémarre le navigateur."""
    pass


async def _launch_browser(p) -> Browser:
    """Lance Firefox (ou Chromium en fallback) en mode headless."""
    try:
        browser = await p.firefox.launch(headless=config.HEADLESS)
        logging.info("Navigateur: Firefox (bypass CDN Akamai)")
        return browser
    except Exception as e:
        logging.warning(f"Firefox unavailable ({e}), fallback sur Chromium")
        browser = await p.chromium.launch(headless=config.HEADLESS)
        logging.info("Navigateur: Chromium (fallback)")
        return browser


async def _setup_context(browser: Browser) -> BrowserContext:
    """Crée un contexte Playwright avec user-agent desktop et blocage des ressources lourdes."""
    context = await browser.new_context(
        user_agent=random.choice(config.USER_AGENTS),
        viewport={"width": 1920, "height": 1080},
    )
    await context.route(
        "**/*",
        lambda route: route.abort()
        if route.request.resource_type in config.BLOCK_RESOURCES
        else route.continue_()
    )
    return context


async def _close_browser_safe(context: Optional[BrowserContext], browser: Optional[Browser]):
    """Ferme contexte + navigateur sans propager d'exception (appelé post-crash)."""
    for obj in (context, browser):
        if obj is not None:
            try:
                await obj.close()
            except Exception:
                pass


# =========================================================
# NAVIGATE WITH RETRY
# =========================================================
async def navigate_with_retry(
    page: Page,
    url: str,
    context: BrowserContext = None,
    max_retries: int = 2,
    timeout: int = None,
) -> bool:
    """Navigate with retry and bounded backoff for transient CDN errors.

    Comportement :
    • Crash Playwright (EPIPE, "browser closed"…) → lève BrowserCrashError immédiatement.
      Réessayer sur un navigateur mort serait inutile et bloquerait le slot pour des minutes.
    • Erreur réseau (timeout, net::ERR_*) → retry avec backoff court (3s, 6s max).
      Backoff intentionnellement court : avec ~3000 offres/run on ne peut pas attendre.
    • Autre erreur → retourne False sans retry (ex: NS_ERROR_FAILURE = page inexistante).
    """
    page_timeout = timeout or config.PAGE_TIMEOUT
    for attempt in range(max_retries):
        try:
            if page.is_closed():
                if context:
                    page = await context.new_page()
                else:
                    return False

            await page.goto(url, timeout=page_timeout, wait_until="load")
            return True

        except Exception as e:
            # Crash navigateur → propagé via exception spéciale pour restart du navigateur
            if _is_browser_crash(e):
                raise BrowserCrashError(f"Browser crash on {url}: {e}")

            error_str = str(e)
            if any(x in error_str.lower() for x in ["err_http2", "net::", "timeout"]):
                # Backoff court : 3s, 6s — on ne peut pas attendre davantage à l'échelle
                wait = min(6, 3 * (2 ** attempt))
                if attempt < max_retries - 1:
                    logging.warning(
                        f"Network error on {url} (attempt {attempt+1}/{max_retries}), "
                        f"retrying in {wait}s"
                    )
                    await asyncio.sleep(wait)
                else:
                    logging.warning(f"Network error on {url}, giving up after {max_retries} attempts")
            else:
                logging.debug(f"Unrecoverable error on {url}: {error_str[:120]}")
                return False

    return False


def _get_listing_url(filter_slug: str, page_num: int) -> str:
    """URL de la page de liste, avec filtre contrat optionnel."""
    if filter_slug:
        return f"{SEARCH_URL}/{filter_slug}?page={page_num}" if page_num > 1 else f"{SEARCH_URL}/{filter_slug}"
    return f"{SEARCH_URL}?q=&page={page_num}" if page_num > 1 else f"{SEARCH_URL}?q="


# =========================================================
# GET TOTAL PAGES
# =========================================================
async def get_total_pages_for_filter(
    context: BrowserContext, filter_slug: str, *, cookie_banner_dismissed: bool = False
) -> int:
    """Retourne le nombre de pages pour un filtre contrat donné."""
    page = await context.new_page()
    url = _get_listing_url(filter_slug, 1)
    await navigate_with_retry(page, url, context=context)

    if not cookie_banner_dismissed:
        try:
            await page.click("button#onetrust-reject-all-handler", timeout=3000)
            logging.info("Cookie banner closed")
        except Exception:
            pass

    await asyncio.sleep(1)
    html = await page.content()
    soup = BeautifulSoup(html, "html.parser")
    try:
        await page.close()
    except Exception:
        pass

    text = soup.get_text()
    match = re.search(r'(\d[\d\s\xa0]*)\s*offres?\s', text)
    if match:
        total = int(match.group(1).replace(' ', '').replace('\xa0', ''))
        pages = max(1, (total + 9) // 10)
        return pages

    max_page = 1
    for link in soup.select('a[href*="page="]'):
        m = re.search(r'page=(\d+)', link.get('href', ''))
        if m:
            max_page = max(max_page, int(m.group(1)))
    return max_page


# =========================================================
# COLLECT JOB URLs FROM LISTING PAGE
# =========================================================
async def fetch_listing_page(
    context: BrowserContext,
    filter_slug: str,
    contract_type: str,
    filter_index: int,
    page_num: int,
    sem: asyncio.Semaphore,
) -> List[Dict]:
    """Récupère les offres d'une page de liste. Le type de contrat vient du filtre URL (fiable)."""
    async with sem:
        page = await context.new_page()
        try:
            url = _get_listing_url(filter_slug, page_num)
            await navigate_with_retry(page, url, context=context)
            await asyncio.sleep(0.4)

            html = await page.content()
            soup = BeautifulSoup(html, "html.parser")

            jobs = []
            for card in soup.select("a.card-link[href*='/emploi-carriere/offre-emploi/']"):
                href = card.get("href", "")
                job_url = BASE_URL + href if href.startswith('/') else href

                h3 = card.select_one("h3.title-4")
                title = h3.get_text(strip=True) if h3 else None

                loc_el = card.select_one("div.offer-location")
                location_raw = loc_el.get_text(strip=True) if loc_el else None

                jobs.append({
                    "job_url": job_url,
                    "job_title": title,
                    "contract_type": contract_type,
                    "location_raw": location_raw,
                    "_filter_index": filter_index,
                })

            return jobs
        except Exception as e:
            logging.error(f"Page {filter_slug} p.{page_num} failed: {e}")
            return []
        finally:
            try:
                await page.close()
            except Exception:
                pass


# =========================================================
# FETCH JOB DETAILS  (robuste aux crashes Playwright)
# =========================================================
async def fetch_job_details(
    context: BrowserContext, job: Dict, sem: asyncio.Semaphore
) -> Dict:
    """Scrape la page de détail d'une offre.

    Lève BrowserCrashError si le navigateur est mort (propagé jusqu'à scrape_details_robust).
    Retourne simplement le dict (possiblement sans données) pour les erreurs réseau ordinaires.
    """
    async with sem:
        page = None
        try:
            page = await context.new_page()
        except Exception as e:
            # new_page() peut échouer sur un navigateur mort → BrowserCrashError
            if _is_browser_crash(e):
                raise BrowserCrashError(f"new_page failed: {e}")
            logging.warning(f"new_page() failed for {job.get('job_url')}: {e}")
            return job

        try:
            success = await navigate_with_retry(page, job["job_url"], context=context)
            # BrowserCrashError est propagée directement si levée dans navigate_with_retry
            if not success:
                return job  # erreur réseau ordinaire, pas de crash — on passe à l'offre suivante

            await asyncio.sleep(0.5)

            html = await page.content()
            soup = BeautifulSoup(html, "html.parser")

            # Title
            h1 = soup.select_one("h1")
            if h1:
                job["job_title"] = h1.get_text(strip=True)

            # Contract type : priorité au type issu du filtre URL
            if not job.get("contract_type"):
                ct_raw = extract_offer_field(soup, "offer-info-type")
                if ct_raw:
                    job["contract_type"] = normalize_contract_type(ct_raw)

            # Location
            loc_raw = extract_offer_field(soup, "offer-info-loc")
            if loc_raw:
                job["location"] = normalize_location(loc_raw)
            elif job.get("location_raw"):
                job["location"] = normalize_location(job["location_raw"])

            # Métier
            metier_raw = extract_offer_field(soup, "offer-info-domain")

            # Référence → job_id
            ref_raw = extract_offer_field(soup, "offer-info-ref")
            if ref_raw:
                job["job_id"] = f"BNP_{ref_raw}"

            # Date (Mise à jour le DD.MM.YYYY)
            date_el = soup.select_one("div.offer-date")
            if date_el:
                date_match = re.search(r'(\d{2})\.(\d{2})\.(\d{4})', date_el.get_text())
                if date_match:
                    day, month, year = date_match.groups()
                    job["publication_date"] = f"{year}-{month}-{day}"

            # Brand / Entité
            brand_name = None
            brand_section = soup.select_one("div.offer-info-rhentity")
            if brand_section:
                brand_link = brand_section.select_one("a")
                if brand_link:
                    brand_text = brand_link.get_text(strip=True)
                    if brand_text:
                        brand_name = brand_text
                    else:
                        img = brand_link.select_one("img")
                        if img and img.get("alt"):
                            brand_name = img["alt"].strip()
                        else:
                            href = brand_link.get("href", "")
                            slug = href.rstrip('/').split('/')[-1]
                            if slug:
                                brand_name = slug.replace('-', ' ').title()

            # Description
            offer_content = soup.select_one(".offer-content")
            if offer_content:
                desc_text = offer_content.get_text(separator=" ", strip=True)
                desc_text = re.sub(r'\s+', ' ', desc_text)
                job["job_description"] = desc_text[:25000]

            # Company description
            company_desc_parts = []
            if brand_name:
                company_desc_parts.append(f"Entité: {brand_name}")
                for h3 in soup.select("h3"):
                    h3_text = h3.get_text(strip=True)
                    if brand_name.lower() in h3_text.lower():
                        next_div = h3.find_next_sibling("div")
                        if next_div:
                            company_desc_parts.append(next_div.get_text(strip=True)[:500])
                        break

            job["company_description"] = (
                " | ".join(company_desc_parts) if company_desc_parts else None
            )
            job["company_name"] = normalize_bnp_brand(brand_name) if brand_name else "BNP Paribas"

            # Education level
            desc_lower = (job.get("job_description") or "").lower()
            if desc_lower:
                education_patterns = [
                    (r'bac\s*\+\s*5|master|mba|phd|doctorat|ingénieur|grande école|école de commerce', "Bac + 5 / M2 et plus"),
                    (r'bac\s*\+\s*4|m1', "Bac + 4 / M1"),
                    (r'bac\s*\+\s*3|bachelor|licence', "Bac + 3 / L3"),
                    (r'bac\s*\+\s*2|bts|dut', "Bac + 2 / L2"),
                    (r'\bbac\b(?!\s*\+)', "Bac"),
                ]
                for pattern, level in education_patterns:
                    if re.search(pattern, desc_lower):
                        job["education_level"] = level
                        break

            # Experience level
            desc = job.get("job_description") or ""
            job["experience_level"] = extract_experience_level(
                desc, job.get("contract_type"), job.get("job_title")
            )

            # Job family classification
            job["job_family"] = classify_job_family(
                job.get("job_title", ""),
                f"{metier_raw or ''} {job.get('job_description', '')}"
            )

            job["status"] = "Live"
            job["technical_skills"] = "[]"
            job["behavioral_skills"] = "[]"

        except BrowserCrashError:
            raise  # remonter jusqu'à _run_detail_batch → scrape_details_robust
        except Exception as e:
            if _is_browser_crash(e):
                raise BrowserCrashError(f"Browser crash mid-detail: {e}")
            logging.warning(f"Detail failed: {job.get('job_url')} ({e})")
        finally:
            # Toujours tenter de fermer — même si le navigateur est mort
            if page is not None:
                try:
                    await page.close()
                except Exception:
                    pass

    return job


# =========================================================
# DETAIL BATCH RUNNER
# =========================================================
def _finalize_job(job_data: Dict, db: JobDatabase):
    """Normalise et sauvegarde immédiatement une offre en DB."""
    if not job_data.get('location') and job_data.get('location_raw'):
        job_data['location'] = normalize_location(job_data['location_raw'])
    job_data.pop('location_raw', None)
    job_data.pop('_crashed', None)

    job_data.setdefault('company_name', 'BNP Paribas')
    job_data.setdefault('status', 'Live')
    job_data.setdefault('technical_skills', '[]')
    job_data.setdefault('behavioral_skills', '[]')

    if not job_data.get('publication_date'):
        existing = db.get_existing_publication_date(job_data.get('job_url', ''))
        job_data['publication_date'] = existing or datetime.now().strftime('%Y-%m-%d')

    db.insert_or_update_job(job_data)


async def _run_detail_batch(
    context: BrowserContext,
    jobs: List[Dict],
    db: JobDatabase,
) -> Tuple[List[str], bool]:
    """Scrape un lot d'offres en parallèle.

    Retourne :
      - completed_urls : URLs sauvegardées avec succès
      - had_crash      : True si au moins un crash Playwright a été détecté
    """
    sem = asyncio.Semaphore(config.MAX_CONCURRENT_DETAILS)
    tasks = [fetch_job_details(context, job, sem) for job in jobs]

    completed_urls: List[str] = []
    had_crash = False

    for coro in tqdm(asyncio.as_completed(tasks), total=len(tasks), desc="Scraping details"):
        try:
            job_data = await coro
        except BrowserCrashError as e:
            logging.warning(f"BrowserCrashError dans le batch: {e}")
            had_crash = True
            continue
        except Exception as e:
            logging.warning(f"Unexpected task error: {e}")
            continue

        _finalize_job(job_data, db)
        completed_urls.append(job_data['job_url'])

    return completed_urls, had_crash


# =========================================================
# ROBUST DETAIL SCRAPING (avec redémarrage navigateur)
# =========================================================
async def scrape_details_robust(p, jobs: List[Dict], db: JobDatabase) -> int:
    """Scrape les détails avec redémarrage automatique du navigateur en cas de crash.

    Stratégie :
      1. Lance un navigateur, scrape un lot.
      2. Si crash détecté → ferme proprement, attend, interroge la DB pour identifier
         les offres déjà sauvegardées, relance sur les restantes.
      3. Répète jusqu'à config.MAX_BROWSER_RESTARTS fois ou épuisement de la liste.

    Retourne le nombre total d'offres scrapées avec succès.
    """
    remaining = list(jobs)
    total_scraped = 0
    max_attempts = config.MAX_BROWSER_RESTARTS + 1

    for attempt in range(max_attempts):
        if not remaining:
            break

        # Après un crash : retirer les offres déjà en DB (pas de double scraping)
        if attempt > 0:
            remaining_urls = {j['job_url'] for j in remaining}
            already_done = db.get_jobs_in_db(remaining_urls)
            extra = len(already_done)
            remaining = [j for j in remaining if j['job_url'] not in already_done]
            total_scraped += extra

            if not remaining:
                logging.info("✅ Toutes les offres sauvegardées après crash+reprise.")
                break

            wait_sec = min(60, 15 * attempt)
            logging.warning(
                f"⚠️  Crash Playwright — redémarrage {attempt}/{config.MAX_BROWSER_RESTARTS}. "
                f"{len(remaining)} offres restantes. Pause {wait_sec}s..."
            )
            await asyncio.sleep(wait_sec)

        logging.info(
            f"🌐 Navigateur — tentative {attempt + 1}/{max_attempts} "
            f"({len(remaining)} offres à scraper)"
        )

        browser: Optional[Browser] = None
        context: Optional[BrowserContext] = None
        try:
            browser = await _launch_browser(p)
            context = await _setup_context(browser)

            completed_urls, had_crash = await _run_detail_batch(context, remaining, db)
            total_scraped += len(completed_urls)

            if not had_crash:
                logging.info(f"✅ Scraping terminé sans crash — {total_scraped} offres au total")
                break

            logging.warning(
                f"⚠️  Crash ce tour : {len(completed_urls)} sauvegardées, "
                f"{len(remaining) - len(completed_urls)} à refaire"
            )

        except BrowserCrashError as e:
            logging.warning(f"BrowserCrashError globale (tentative {attempt + 1}): {e}")
        except Exception as e:
            logging.error(f"Erreur inattendue (tentative {attempt + 1}): {e}")

        finally:
            await _close_browser_safe(context, browser)

    else:
        logging.error(
            f"🔴 Limite de {config.MAX_BROWSER_RESTARTS} redémarrages atteinte. "
            "Certaines offres peuvent être manquantes."
        )

    return total_scraped


# =========================================================
# MAIN PIPELINE
# =========================================================
async def main():
    start = time.time()
    logging.info("=" * 80)
    logging.info("DÉBUT PIPELINE BNP PARIBAS JOB SCRAPER")
    logging.info("=" * 80)

    db = JobDatabase(config.DB_PATH)
    logging.info(f"Base de données initialisée: {config.DB_PATH}")
    logging.info(
        f"Concurrence — listing: {config.MAX_CONCURRENT_LISTING}, "
        f"détails: {config.MAX_CONCURRENT_DETAILS}"
    )

    async with async_playwright() as p:

        # ── Étape 1 : Collecter tous les liens par type de contrat ─────────────
        logging.info("\n📋 ÉTAPE 1: Collection des liens par type de contrat")

        # Navigateur dédié au listing (fermé avant la phase détail pour libérer mémoire)
        listing_browser = await _launch_browser(p)
        listing_context = await _setup_context(listing_browser)

        sem_pages = asyncio.Semaphore(config.MAX_CONCURRENT_LISTING)
        page_tasks = []
        cookie_done = False

        for idx, (filter_slug, contract_type) in enumerate(CONTRACT_FILTERS):
            try:
                total_pages = await get_total_pages_for_filter(
                    listing_context, filter_slug, cookie_banner_dismissed=cookie_done
                )
                cookie_done = True
                logging.info(f"  {contract_type}: {total_pages} pages")
                for pg in range(1, total_pages + 1):
                    page_tasks.append(
                        fetch_listing_page(listing_context, filter_slug, contract_type, idx, pg, sem_pages)
                    )
            except Exception as e:
                logging.warning(f"  {contract_type} ({filter_slug}): {e}")

        all_jobs_basic = []
        for coro in tqdm(
            asyncio.as_completed(page_tasks), total=len(page_tasks), desc="Collecting URLs"
        ):
            page_jobs = await coro
            all_jobs_basic.extend(page_jobs)

        # Fermer le navigateur listing avant la phase détail
        await _close_browser_safe(listing_context, listing_browser)

        # Dédupliquer par URL : garder le type le plus spécifique
        seen = {}
        for job in all_jobs_basic:
            url = job["job_url"]
            idx = job.get("_filter_index", 0)
            if url not in seen or idx > seen[url].get("_filter_index", -1):
                seen[url] = job
        for j in seen.values():
            j.pop("_filter_index", None)
        unique_jobs = list(seen.values())

        all_current_urls = {j["job_url"] for j in unique_jobs}
        logging.info(f"Total unique URLs collectées: {len(all_current_urls)}")

        # ── Étape 2 : Identifier nouveaux et expirés ───────────────────────────
        logging.info("\n🔍 ÉTAPE 2: Analyse des changements")
        existing_live_urls = db.get_live_urls()

        new_urls = all_current_urls - existing_live_urls
        expired_urls = existing_live_urls - all_current_urls

        logging.info(f"✅ Nouvelles offres: {len(new_urls)}")
        logging.info(f"❌ Offres expirées: {len(expired_urls)}")

        # ── Étape 3 : Marquer les expirées ────────────────────────────────────
        if expired_urls:
            logging.info("\n⏳ ÉTAPE 3: Marquage des offres expirées")
            db.mark_as_expired(expired_urls)
            logging.info(f"✓ {len(expired_urls)} offres marquées comme expirées")

        # ── Étape 4 : Scraper les détails des nouvelles offres ─────────────────
        if new_urls:
            new_jobs = [j for j in unique_jobs if j["job_url"] in new_urls]
            logging.info(f"\n🚀 ÉTAPE 4: Scraping de {len(new_jobs)} nouvelles offres")

            total_scraped = await scrape_details_robust(p, new_jobs, db)
            logging.info(f"✓ {total_scraped} nouvelles offres scrapées au total")
        else:
            logging.info("\n✓ Aucune nouvelle offre à scraper")

    # ── Étape 5 : Export CSV ──────────────────────────────────────────────────
    logging.info("\n💾 ÉTAPE 5: Export vers CSV")
    db.export_to_csv(config.CSV_PATH)
    logging.info(f"✓ CSV exporté: {config.CSV_PATH}")

    # ── Statistiques finales ──────────────────────────────────────────────────
    with sqlite3.connect(config.DB_PATH) as conn:
        cursor = conn.execute("""
            SELECT
                COUNT(*) as total,
                SUM(CASE WHEN status = 'Live' THEN 1 ELSE 0 END) as live,
                SUM(CASE WHEN status = 'Expired' THEN 1 ELSE 0 END) as expired,
                SUM(CASE WHEN is_valid = 0 THEN 1 ELSE 0 END) as invalid
            FROM jobs
        """)
        stats = cursor.fetchone()

        logging.info("\n" + "=" * 60)
        logging.info("📊 STATISTIQUES FINALES")
        logging.info("=" * 60)
        logging.info(f"Total d'offres en base: {stats[0]}")
        logging.info(f"  └─ Live (actives): {stats[1]}")
        logging.info(f"  └─ Expired (expirées): {stats[2]}")
        logging.info(f"  └─ Invalid: {stats[3]}")
        logging.info("=" * 60)

    elapsed = time.time() - start
    logging.info(f"Time elapsed: {elapsed:.2f}s")
    logging.info("=" * 80)


if __name__ == "__main__":
    asyncio.run(main())
