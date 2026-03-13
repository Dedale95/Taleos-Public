#!/usr/bin/env python3
"""
BNP PARIBAS - JOB SCRAPER
Pipeline optimisé pour scraper les offres d'emploi du groupe BNP Paribas.
Utilise Playwright (async) + BeautifulSoup pour contourner les protections anti-bot.
"""

import asyncio
import logging
import re
import time
import sqlite3
import json
import pandas as pd
from pathlib import Path
from typing import List, Dict, Set, Optional
from playwright.async_api import async_playwright, BrowserContext
from bs4 import BeautifulSoup
from tqdm.asyncio import tqdm
from urllib.parse import urljoin
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

# ================= Config =================
class Config:
    MAX_CONCURRENT_LISTING = 12   # Pages de liste (légères)
    MAX_CONCURRENT_DETAILS = 12   # Pages détail (augmenté pour ~2600 offres)
    PAGE_TIMEOUT = 30000
    WAIT_TIMEOUT = 10000
    HEADLESS = True
    BASE_DIR = Path(__file__).parent
    DB_PATH = BASE_DIR / "bnp_paribas_jobs.db"
    CSV_PATH = BASE_DIR / "bnp_paribas_jobs.csv"

    BLOCK_RESOURCES = {
        "image", "font", "media", "texttrack",
        "object", "beacon", "csp_report", "imageset"
    }

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
    "graduate programme (cdi)": "Graduate Programme",
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
}


def normalize_bnp_brand(raw: str) -> str:
    """Normalise le nom de marque BNP pour affichage cohérent (Arval, BNP Paribas Cardif, etc.)."""
    if not raw or not raw.strip():
        return "BNP Paribas"
    t = raw.strip()
    # Retirer le suffixe "(Groupe BNP Paribas)" si présent
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
# GET TOTAL PAGES
# =========================================================
async def navigate_with_retry(page, url: str, max_retries: int = 6):
    """Navigate with retry and exponential backoff for transient errors (CDN blocks)."""
    for attempt in range(max_retries):
        try:
            await page.goto(url, timeout=config.PAGE_TIMEOUT, wait_until="domcontentloaded")
            return True
        except Exception as e:
            error_str = str(e)
            if "ERR_HTTP2" in error_str or "net::" in error_str or "timeout" in error_str.lower():
                wait = 15 * (2 ** attempt)  # 15s, 30s, 60s, 120s, 240s, 480s
                logging.warning(f"Network error on {url} (attempt {attempt+1}/{max_retries}), retrying in {wait}s...")
                await asyncio.sleep(wait)
            else:
                raise
    raise Exception(f"Failed after {max_retries} retries: {url}")


async def get_total_pages(context: BrowserContext) -> int:
    page = await context.new_page()
    await navigate_with_retry(page, f"{SEARCH_URL}?q=")

    try:
        await page.click("button#onetrust-reject-all-handler", timeout=3000)
        logging.info("Cookie banner closed")
    except:
        pass

    await asyncio.sleep(1)  # Cookie banner + rendu
    html = await page.content()
    soup = BeautifulSoup(html, "html.parser")
    await page.close()

    text = soup.get_text()
    match = re.search(r'(\d[\d\s\xa0]*)\s*offres?\s', text)
    if match:
        total = int(match.group(1).replace(' ', '').replace('\xa0', ''))
        pages = (total + 9) // 10
        logging.info(f"Total offres détectées: {total} → {pages} pages")
        return pages

    max_page = 1
    for link in soup.select('a[href*="page="]'):
        m = re.search(r'page=(\d+)', link.get('href', ''))
        if m:
            max_page = max(max_page, int(m.group(1)))
    logging.info(f"Pages détectées via pagination: {max_page}")
    return max_page


# =========================================================
# COLLECT JOB URLs FROM LISTING PAGE
# =========================================================
async def fetch_listing_page(
    context: BrowserContext, page_num: int, sem: asyncio.Semaphore
) -> List[Dict]:
    async with sem:
        page = await context.new_page()
        try:
            url = f"{SEARCH_URL}?q=&page={page_num}"
            await navigate_with_retry(page, url)
            await asyncio.sleep(0.4)  # Légère pause pour le rendu (domcontentloaded suffit)

            html = await page.content()
            soup = BeautifulSoup(html, "html.parser")

            jobs = []
            for card in soup.select("a.card-link[href*='/emploi-carriere/offre-emploi/']"):
                href = card.get("href", "")
                job_url = BASE_URL + href if href.startswith('/') else href

                h3 = card.select_one("h3.title-4")
                title = h3.get_text(strip=True) if h3 else None

                ct_el = card.select_one("div.offer-type")
                contract_raw = ct_el.get_text(strip=True) if ct_el else None
                contract_type = normalize_contract_type(contract_raw) if contract_raw else None

                loc_el = card.select_one("div.offer-location")
                location_raw = loc_el.get_text(strip=True) if loc_el else None

                jobs.append({
                    "job_url": job_url,
                    "job_title": title,
                    "contract_type": contract_type,
                    "location_raw": location_raw,
                })

            return jobs
        except Exception as e:
            logging.error(f"Page {page_num} failed: {e}")
            return []
        finally:
            await page.close()


# =========================================================
# FETCH JOB DETAILS
# =========================================================
async def fetch_job_details(
    context: BrowserContext, job: Dict, sem: asyncio.Semaphore
) -> Dict:
    async with sem:
        page = await context.new_page()
        try:
            await navigate_with_retry(page, job["job_url"])
            await asyncio.sleep(0.4)  # Légère pause pour le rendu

            html = await page.content()
            soup = BeautifulSoup(html, "html.parser")

            # Title
            h1 = soup.select_one("h1")
            if h1:
                job["job_title"] = h1.get_text(strip=True)

            # Contract type
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

            # Company description (brand section at bottom)
            company_desc_parts = []
            if brand_name:
                company_desc_parts.append(f"Entité: {brand_name}")
                for h3 in soup.select("h3"):
                    h3_text = h3.get_text(strip=True)
                    if brand_name.lower() in h3_text.lower():
                        next_div = h3.find_next_sibling("div")
                        if next_div:
                            company_desc_parts.append(
                                next_div.get_text(strip=True)[:500]
                            )
                        break

            job["company_description"] = (
                " | ".join(company_desc_parts) if company_desc_parts else None
            )
            # Utiliser la marque extraite pour distinguer les entités du groupe (Arval, BNP Paribas Cardif, etc.)
            job["company_name"] = normalize_bnp_brand(brand_name) if brand_name else "BNP Paribas"

            # Education level (from description)
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

            # Experience level (module partagé)
            desc = job.get("job_description") or ""
            job["experience_level"] = extract_experience_level(desc, job.get("contract_type"), job.get("job_title"))

            # Job family classification
            job["job_family"] = classify_job_family(
                job.get("job_title", ""),
                f"{metier_raw or ''} {job.get('job_description', '')}"
            )

            job["status"] = "Live"
            job["technical_skills"] = "[]"
            job["behavioral_skills"] = "[]"

        except Exception as e:
            logging.warning(f"Detail failed: {job.get('job_url')} ({e})")
        finally:
            await page.close()

    return job


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

    async with async_playwright() as p:
        # Firefox a une empreinte TLS différente - contourne parfois les blocages CDN (Akamai)
        try:
            browser = await p.firefox.launch(headless=config.HEADLESS)
            logging.info("Using Firefox (bypass CDN)")
        except Exception:
            browser = await p.chromium.launch(headless=config.HEADLESS)
            logging.info("Using Chromium (Firefox fallback)")
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            viewport={"width": 1920, "height": 1080},
        )

        await context.route(
            "**/*",
            lambda route: route.abort()
            if route.request.resource_type in config.BLOCK_RESOURCES
            else route.continue_()
        )

        # ── Étape 1 : Collecter tous les liens ──
        logging.info("\n📋 ÉTAPE 1: Collection des liens d'offres")
        total_pages = await get_total_pages(context)

        sem_pages = asyncio.Semaphore(config.MAX_CONCURRENT_LISTING)
        page_tasks = [
            fetch_listing_page(context, p, sem_pages)
            for p in range(1, total_pages + 1)
        ]

        all_jobs_basic = []
        for coro in tqdm(
            asyncio.as_completed(page_tasks), total=total_pages, desc="Collecting URLs"
        ):
            page_jobs = await coro
            all_jobs_basic.extend(page_jobs)

        seen = set()
        unique_jobs = []
        for job in all_jobs_basic:
            if job["job_url"] not in seen:
                seen.add(job["job_url"])
                unique_jobs.append(job)

        all_current_urls = {j["job_url"] for j in unique_jobs}
        logging.info(f"Total unique URLs collectées: {len(all_current_urls)}")

        # ── Étape 2 : Identifier nouveaux et expirés ──
        logging.info("\n🔍 ÉTAPE 2: Analyse des changements")
        existing_live_urls = db.get_live_urls()

        new_urls = all_current_urls - existing_live_urls
        expired_urls = existing_live_urls - all_current_urls

        logging.info(f"✅ Nouvelles offres: {len(new_urls)}")
        logging.info(f"❌ Offres expirées: {len(expired_urls)}")

        # ── Étape 3 : Marquer les expirées ──
        if expired_urls:
            logging.info("\n⏳ ÉTAPE 3: Marquage des offres expirées")
            db.mark_as_expired(expired_urls)
            logging.info(f"✓ {len(expired_urls)} offres marquées comme expirées")

        # ── Étape 4 : Scraper les détails des nouvelles offres ──
        if new_urls:
            new_jobs = [j for j in unique_jobs if j["job_url"] in new_urls]
            logging.info(f"\n🚀 ÉTAPE 4: Scraping de {len(new_jobs)} nouvelles offres")

            sem_jobs = asyncio.Semaphore(config.MAX_CONCURRENT_DETAILS)
            job_tasks = [
                fetch_job_details(context, job, sem_jobs) for job in new_jobs
            ]

            for coro in tqdm(
                asyncio.as_completed(job_tasks),
                total=len(job_tasks),
                desc="Scraping details",
            ):
                job_data = await coro
                if job_data:
                    if not job_data.get("location") and job_data.get("location_raw"):
                        job_data["location"] = normalize_location(
                            job_data["location_raw"]
                        )
                    job_data.pop("location_raw", None)

                    job_data.setdefault("company_name", "BNP Paribas")
                    job_data.setdefault("status", "Live")
                    job_data.setdefault("technical_skills", "[]")
                    job_data.setdefault("behavioral_skills", "[]")

                    if not job_data.get("publication_date"):
                        existing = db.get_existing_publication_date(
                            job_data.get("job_url", "")
                        )
                        job_data["publication_date"] = (
                            existing or datetime.now().strftime("%Y-%m-%d")
                        )

                    db.insert_or_update_job(job_data)
        else:
            logging.info("\n✓ Aucune nouvelle offre à scraper")

        await context.close()
        await browser.close()

    # ── Étape 5 : Export CSV ──
    logging.info("\n💾 ÉTAPE 5: Export vers CSV")
    db.export_to_csv(config.CSV_PATH)
    logging.info(f"✓ CSV exporté: {config.CSV_PATH}")

    # ── Statistiques finales ──
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
