#!/usr/bin/env python3
"""
BPI FRANCE - JOB SCRAPER
Extrait les offres d'emploi du site talents.bpifrance.fr
Utilise Playwright (le site bloque les requêtes sans navigateur).
"""

import asyncio
import logging
import re
import time
import sqlite3
import json
import pandas as pd
from pathlib import Path
from typing import List, Dict, Optional, Set
from datetime import datetime
from urllib.parse import urljoin, urlparse, parse_qs
from html import unescape

from playwright.async_api import async_playwright, BrowserContext
from bs4 import BeautifulSoup
from tqdm.asyncio import tqdm

try:
    from city_normalizer import normalize_city
    from country_normalizer import normalize_country, get_country_from_city
    from job_family_classifier import classify_job_family
except ImportError:
    import sys
    sys.path.append(str(Path(__file__).parent))
    from city_normalizer import normalize_city
    from country_normalizer import normalize_country, get_country_from_city
    from job_family_classifier import classify_job_family

# ================= Logging =================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# ================= Constants =================
BASE_URL = "https://talents.bpifrance.fr"
LISTING_URL = f"{BASE_URL}/nos-opportunites/"
LISTING_PAGE_URL = f"{BASE_URL}/nos-opportunites/?paged={{}}"

# ================= Config =================
class Config:
    MAX_CONCURRENT_PAGES = 5
    PAGE_TIMEOUT = 30000
    WAIT_TIMEOUT = 10000
    HEADLESS = True
    BASE_DIR = Path(__file__).parent
    DB_PATH = BASE_DIR / "bpifrance_jobs.db"
    CSV_PATH = BASE_DIR / "bpifrance_jobs.csv"

    BLOCK_RESOURCES = {
        "image", "font", "media", "texttrack",
        "object", "beacon", "csp_report", "imageset"
    }

config = Config()

# ================= Contract mapping =================
CONTRACT_MAPPING = {
    "cdi": "CDI",
    "cdd": "CDD",
    "stage": "Stage",
    "alternance": "Alternance / Apprentissage",
    "vie": "VIE",
    "v.i.e": "VIE",
}


def normalize_contract(raw: Optional[str]) -> Optional[str]:
    if not raw:
        return None
    cleaned = str(raw).strip().lower()
    for key, value in CONTRACT_MAPPING.items():
        if key in cleaned:
            return value
    return raw.strip()


def build_location(location_raw: Optional[str]) -> Optional[str]:
    """Construit 'Ville - Pays' à partir du texte brut (ex: 'Paris (haussmann)', 'Strasbourg')."""
    if not location_raw:
        return None
    # Nettoyer: "Paris (haussmann)" -> "Paris", "Maisons-alfort" -> "Maisons-Alfort"
    loc = location_raw.strip()
    # Retirer les parenthèses (quartier/bureau)
    loc = re.sub(r'\s*\([^)]+\)\s*', ' ', loc).strip()
    city = normalize_city(loc) if loc else None
    if not city:
        return None
    country = get_country_from_city(city) or "France"
    country = normalize_country(country)
    if city.lower() == country.lower():
        return country
    return f"{city} - {country}"


# =========================================================
# DATABASE MANAGER
# =========================================================
class JobDatabase:
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
                technical_skills = '[]'
            if isinstance(behavioral_skills, list):
                behavioral_skills = json.dumps(behavioral_skills, ensure_ascii=False)
            elif not isinstance(behavioral_skills, str) or not behavioral_skills.startswith('['):
                behavioral_skills = '[]'

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
                    publication_date = COALESCE(NULLIF(TRIM(COALESCE(excluded.publication_date,'')), ''), jobs.publication_date),
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
                        if x and isinstance(x, str) and x.startswith('[') else x
                    )
            df.to_csv(csv_path, index=False, encoding='utf-8')


# =========================================================
# EXTRACT EDUCATION & EXPERIENCE FROM DESCRIPTION
# =========================================================
def extract_education_level(text: str) -> Optional[str]:
    if not text:
        return None
    text_lower = text.lower()
    patterns = [
        (r'bac\s*\+\s*5|master|mba|phd|doctorat|ingénieur|engineer|grande école|école d\'ingénieur|école de commerce', "Bac + 5 / M2 et plus"),
        (r'bac\s*\+\s*4|m1', "Bac + 4 / M1"),
        (r'bac\s*\+\s*3|bachelor|licence|l3', "Bac + 3 / L3"),
        (r'bac\s*\+\s*2|l2|bts|dut', "Bac + 2 / L2"),
        (r'\bbac\b(?!\s*\+)', "Bac"),
    ]
    for pattern, level in patterns:
        if re.search(pattern, text_lower):
            return level
    return None


def extract_experience_level(text: str, contract_type: Optional[str]) -> Optional[str]:
    if contract_type in ['Stage', 'VIE', 'Alternance / Apprentissage']:
        return "0 - 2 ans"
    if not text:
        return None
    text_lower = text.lower()
    patterns = [
        (r'(?:plus de|more than|over)\s*(?:10|11|15|20)\s*(?:ans|years?)', "11 ans et plus"),
        (r'(?:10|11|12|15)\+?\s*(?:ans|years?)', "11 ans et plus"),
        (r'senior|confirmé|confirmed', "11 ans et plus"),
        (r'(?:6|7|8|9|10)\s*(?:-|à|to)\s*(?:10|11|12)\s*(?:ans|years?)', "6 - 10 ans"),
        (r'(?:3|4|5)\s*(?:-|à|to)\s*(?:5|6|7)\s*(?:ans|years?)', "3 - 5 ans"),
        (r'(?:0|1|2)\s*(?:-|à|to)\s*(?:2|3)\s*(?:ans|years?)', "0 - 2 ans"),
        (r'junior|débutant|beginner|entry|jeune diplômé|stagiaire|alternant', "0 - 2 ans"),
    ]
    for pattern, level in patterns:
        if re.search(pattern, text_lower):
            return level
    return None


# =========================================================
# FETCH ALL JOBS FROM LISTING PAGES
# =========================================================
async def get_total_pages(context: BrowserContext) -> int:
    """Détecte le nombre total de pages de la liste."""
    page = await context.new_page()
    try:
        await page.goto(LISTING_URL, timeout=config.PAGE_TIMEOUT, wait_until="domcontentloaded")
        try:
            await page.click("button:has-text('Tout refuser'), button:has-text('Refuser'), [aria-label='Fermer']", timeout=3000)
        except Exception:
            pass
        await page.wait_for_selector("a[href*='/opportunites/']", timeout=config.WAIT_TIMEOUT)
        await asyncio.sleep(2)
        html = await page.content()
        soup = BeautifulSoup(html, "html.parser")
        max_page = 1
        # Méthode 1: liens avec href /page/N/
        for a in soup.select("a[href*='/page/']"):
            href = a.get("href", "")
            m = re.search(r'/page/(\d+)/?', href)
            if m:
                max_page = max(max_page, int(m.group(1)))
        # Méthode 2: texte des liens de pagination (ex: "41" pour dernière page)
        if max_page == 1:
            for a in soup.select("a"):
                text = a.get_text(strip=True)
                if text.isdigit() and int(text) > 1:
                    max_page = max(max_page, int(text))
        # Limiter à 50 pages max (sécurité)
        max_page = min(max_page, 50)
        logging.info(f"Pages détectées: {max_page}")
        return max_page
    except Exception as e:
        logging.warning(f"Erreur détection pagination: {e}")
        return 1
    finally:
        await page.close()


async def fetch_listing_page(context: BrowserContext, page_num: int, sem: asyncio.Semaphore) -> List[Dict]:
    """Récupère les jobs d'une page de liste."""
    async with sem:
        page = await context.new_page()
        try:
            url = LISTING_PAGE_URL.format(page_num)
            await page.goto(url, timeout=config.PAGE_TIMEOUT, wait_until="domcontentloaded")
            await page.wait_for_selector("a[href*='/opportunites/']", timeout=config.WAIT_TIMEOUT)
            await asyncio.sleep(1)
            html = await page.content()
            soup = BeautifulSoup(html, "html.parser")
            jobs = []
            seen_urls = set()

            # Structure: h3 (titre) suivi de a "Contract • Location Voir le détail"
            for link in soup.select("a[href*='/opportunites/'][href*='id=']"):
                href = link.get("href", "")
                if not href or "id=" not in href:
                    continue
                full_url = urljoin(BASE_URL, href)
                if full_url in seen_urls:
                    continue
                seen_urls.add(full_url)

                # Job ID depuis l'URL (?id=9467-05)
                parsed = urlparse(full_url)
                params = parse_qs(parsed.query)
                job_id_raw = params.get("id", [None])[0]
                job_id = "BPI_" + job_id_raw if job_id_raw else None

                # Titre: h3 qui précède le lien (structure BPI: h3 puis a)
                job_title = None
                h3 = link.find_previous_sibling("h3") or link.find_previous("h3")
                if h3:
                    job_title = h3.get_text(strip=True)

                # Contrat et lieu: texte du lien "CDI • Paris (haussmann) Voir le détail"
                link_text = link.get_text(strip=True).replace("Voir le détail", "").strip()
                contract_type = None
                location_raw = None
                if " • " in link_text:
                    parts = link_text.split(" • ", 1)
                    contract_type = normalize_contract(parts[0].strip()) if parts else None
                    location_raw = parts[1].strip() if len(parts) > 1 else None

                location = build_location(location_raw) if location_raw else None
                if not job_title:
                    job_title = "Sans titre"

                jobs.append({
                    "job_url": full_url,
                    "job_id": job_id,
                    "job_title": job_title if job_title and len(job_title) > 3 else "Sans titre",
                    "contract_type": contract_type,
                    "location": location,
                    "company_name": "Bpifrance",
                    "status": "Live",
                })
            return jobs
        except Exception as e:
            logging.error(f"Erreur page {page_num}: {e}")
            return []
        finally:
            await page.close()


# =========================================================
# FETCH JOB DETAIL (description complète)
# =========================================================
async def fetch_job_detail(context: BrowserContext, job: Dict, sem: asyncio.Semaphore) -> None:
    """Enrichit un job avec la description depuis la page détail."""
    async with sem:
        page = await context.new_page()
        try:
            await page.goto(job["job_url"], timeout=config.PAGE_TIMEOUT, wait_until="domcontentloaded")
            await asyncio.sleep(1)
            html = await page.content()
            soup = BeautifulSoup(html, "html.parser")

            # Titre (au cas où pas extrait correctement en liste)
            h1 = soup.select_one("h1")
            if h1:
                job["job_title"] = h1.get_text(strip=True)

            # Métadonnées header: "cdi • temps-plein • Paris (Haussmann)"
            header_meta = soup.select_one(".entry-header .meta, .job-meta, .offer-meta, [class*='meta']")
            if header_meta:
                meta_text = header_meta.get_text(strip=True).lower()
                if " • " in meta_text:
                    parts = meta_text.split(" • ")
                    if not job.get("contract_type") and parts:
                        job["contract_type"] = normalize_contract(parts[0])
                    if not job.get("location") and len(parts) > 1:
                        loc_raw = " ".join(parts[1:])
                        job["location"] = build_location(loc_raw)

            # Description: contenu principal (missions, profil, etc.)
            main = soup.select_one("article .entry-content, .job-description, .offer-content, [class*='content']")
            if main:
                # Exclure les blocs "Ces offres peuvent aussi vous intéresser" et similaires
                for skip in main.select(".related-jobs, .similar-offers, [class*='related']"):
                    skip.decompose()
                desc_text = main.get_text(separator="\n", strip=True)
                desc_text = unescape(desc_text)
                desc_text = re.sub(r'\s+', ' ', desc_text).strip()[:25000]
                job["job_description"] = desc_text
            else:
                job["job_description"] = ""

            # Job family, education, experience
            desc = job.get("job_description", "")
            job["job_family"] = classify_job_family(job.get("job_title", ""), desc) if desc else None
            job["education_level"] = extract_education_level(desc)
            job["experience_level"] = extract_experience_level(desc, job.get("contract_type"))

        except Exception as e:
            logging.warning(f"Erreur détail {job.get('job_url')}: {e}")
        finally:
            await page.close()


# =========================================================
# MAIN
# =========================================================
async def main():
    start = time.time()
    logging.info("=" * 80)
    logging.info("DÉBUT PIPELINE BPI FRANCE JOB SCRAPER")
    logging.info("=" * 80)

    db = JobDatabase(config.DB_PATH)
    logging.info(f"Base de données: {config.DB_PATH}")

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=config.HEADLESS)
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )
        # Ne pas bloquer les ressources pour BPI (pagination peut en dépendre)
        # await context.route("**/*", lambda r: r.abort() if r.request.resource_type in config.BLOCK_RESOURCES else r.continue_())

        # Étape 1: Collecter tous les jobs (liste)
        logging.info("\n📋 ÉTAPE 1: Collection des offres")
        total_pages = await get_total_pages(context)
        sem = asyncio.Semaphore(config.MAX_CONCURRENT_PAGES)
        all_jobs = []
        for page_num in range(1, total_pages + 1):
            page_jobs = await fetch_listing_page(context, page_num, sem)
            if not page_jobs and page_num > 1:
                logging.info(f"Page {page_num} vide, arrêt.")
                break
            for j in page_jobs:
                if not any(x["job_url"] == j["job_url"] for x in all_jobs):
                    all_jobs.append(j)
        logging.info(f"Offres collectées: {len(all_jobs)}")

        all_current_links = {j["job_url"] for j in all_jobs if j.get("job_url")}

        # Étape 2: Nouveaux vs expirés
        logging.info("\n🔍 ÉTAPE 2: Analyse des changements")
        existing_live_urls = db.get_live_urls()
        new_urls = all_current_links - existing_live_urls
        expired_urls = existing_live_urls - all_current_links
        logging.info(f"✅ Nouvelles offres: {len(new_urls)}")
        logging.info(f"❌ Offres expirées: {len(expired_urls)}")

        # Étape 3: Marquer expirées
        if expired_urls:
            logging.info("\n⏳ ÉTAPE 3: Marquage des offres expirées")
            db.mark_as_expired(expired_urls)

        # Étape 4: Scraper les détails des nouveaux
        new_jobs = [j for j in all_jobs if j.get("job_url") in new_urls]
        if new_jobs:
            logging.info(f"\n🚀 ÉTAPE 4: Scraping de {len(new_jobs)} nouvelles offres")
            tasks = [fetch_job_detail(context, job, sem) for job in new_jobs]
            for coro in tqdm(asyncio.as_completed(tasks), total=len(tasks), desc="Détails"):
                await coro
            today = datetime.now().strftime("%Y-%m-%d")
            for job in new_jobs:
                job["publication_date"] = db.get_existing_publication_date(job.get("job_url", "")) or today
                db.insert_or_update_job(job)
        else:
            logging.info("\n✓ Aucune nouvelle offre à scraper")

        await context.close()
        await browser.close()

    # Étape 5: Export CSV
    logging.info("\n💾 ÉTAPE 5: Export vers CSV")
    db.export_to_csv(config.CSV_PATH)

    with sqlite3.connect(config.DB_PATH) as conn:
        stats = conn.execute("""
            SELECT 
                COUNT(*) as total,
                SUM(CASE WHEN status = 'Live' THEN 1 ELSE 0 END) as live,
                SUM(CASE WHEN status = 'Expired' THEN 1 ELSE 0 END) as expired
            FROM jobs
        """).fetchone()

    logging.info("\n" + "=" * 60)
    logging.info("📊 STATISTIQUES FINALES")
    logging.info("=" * 60)
    logging.info(f"Total: {stats[0]} | Live: {stats[1]} | Expired: {stats[2]}")
    logging.info("=" * 60)
    logging.info(f"Temps: {time.time() - start:.2f}s")
    logging.info("=" * 80)


if __name__ == "__main__":
    asyncio.run(main())
