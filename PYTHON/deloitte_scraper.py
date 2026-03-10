#!/usr/bin/env python3
"""
DELOITTE - JOB SCRAPER
Extracts job data from Deloitte France careers page.
Uses Playwright for JS execution and parallel detail extraction.
"""

import asyncio
import logging
import csv
import re
import time
import sqlite3
import json
import pandas as pd
from pathlib import Path
from typing import List, Dict, Optional, Set
from playwright.async_api import async_playwright, BrowserContext, Page
from bs4 import BeautifulSoup
from tqdm.asyncio import tqdm
from urllib.parse import urljoin, urlparse, parse_qs
from datetime import datetime
from city_normalizer import normalize_city
from country_normalizer import normalize_country, get_country_from_city
from job_family_classifier import classify_job_family

# ================= Logging =================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# ================= Constants =================
BASE_URL = "https://www.deloitte.com"
SEARCH_URL = "https://www.deloitte.com/fr/fr/careers/content/job/results.html"

# ================= Config =================
class Config:
    MAX_CONCURRENT_PAGES = 5
    PAGE_TIMEOUT = 30000
    WAIT_TIMEOUT = 5000
    HEADLESS = True
    BASE_DIR = Path(__file__).parent
    DB_PATH = BASE_DIR / "deloitte_jobs.db"
    CSV_PATH = BASE_DIR / "deloitte_jobs.csv"

    BLOCK_RESOURCES = {
        "image", "font", "media", "texttrack",
        "object", "beacon", "csp_report", "imageset"
    }

config = Config()

# =========================================================
# DATABASE MANAGER
# =========================================================
class JobDatabase:
    """Gestion de la base de données SQLite pour Deloitte"""

    def __init__(self, db_path: Path):
        self.db_path = db_path
        self.init_db()

    def init_db(self):
        """Initialise la structure de la base de données"""
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
        """Récupère les URLs avec status='Live'"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute("SELECT job_url FROM jobs WHERE status = 'Live' AND is_valid = 1")
            return {row[0] for row in cursor.fetchall()}

    def get_existing_publication_date(self, job_url: str) -> Optional[str]:
        """Récupère la date de publication existante (pour ne jamais l'écraser au re-scrape).
        Utilise publication_date si présente, sinon first_seen (date du premier scrape)."""
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
                # first_seen est un timestamp (ex: 2026-01-15 12:34:56) -> extraire date
                s = str(first_seen).strip()
                return s[:10] if len(s) >= 10 else s
            return None

    def mark_as_expired(self, urls: Set[str]):
        """Marque des offres comme expirées"""
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
        """Insert ou update un job"""
        with sqlite3.connect(self.db_path) as conn:
            # Vérifier si le job a du contenu valide
            is_valid = 1 if (job.get('job_id') or job.get('job_title') or job.get('job_description')) else 0

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
                job.get('training_specialization'), job.get('technical_skills'),
                job.get('behavioral_skills'), job.get('tools'),
                job.get('languages'), job.get('job_description'),
                job.get('company_name'), job.get('company_description'), is_valid
            ))
            conn.commit()

    def export_to_csv(self, csv_path: Path):
        """Export les données valides vers CSV"""
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
            df.to_csv(csv_path, index=False, encoding='utf-8')

# =========================================================
# GET TOTAL RESULTS AND ALL JOBS IN ONE GO
# =========================================================
async def get_all_jobs(context: BrowserContext) -> List[Dict]:
    page = await context.new_page()
    logging.info(f"Navigating to {SEARCH_URL} to get total count...")
    
    await page.goto(SEARCH_URL, timeout=config.PAGE_TIMEOUT, wait_until="networkidle")
    
    # Handle cookies if present
    try:
        await page.click("#didomi-notice-disagree-button", timeout=3000)
        logging.info("Cookie banner closed")
    except:
        pass

    # Wait for the results header to appear
    try:
        await page.wait_for_selector("h2.filters-module__nb_results__PNDl7", timeout=10000)
    except:
        logging.error("Could not find results count selector.")
        await page.close()
        return []

    html = await page.content()
    soup = BeautifulSoup(html, "html.parser")
    
    count_text = soup.select_one("h2.filters-module__nb_results__PNDl7").get_text(strip=True)
    match = re.search(r"(\d+)", count_text)
    total_count = int(match.group(1)) if match else 0
    logging.info(f"Detected {total_count} job offers.")

    if total_count == 0:
        await page.close()
        return []

    # Reload with limit
    full_url = f"{SEARCH_URL}?limit={total_count + 50}"
    logging.info(f"Reloading with limit: {full_url}")
    await page.goto(full_url, timeout=config.PAGE_TIMEOUT, wait_until="networkidle")
    
    # Scroll to ensure all are loaded if lazy loading is involved (Deloitte seems to load all with limit)
    await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
    await asyncio.sleep(2)

    html = await page.content()
    soup = BeautifulSoup(html, "html.parser")
    await page.close()

    job_cards = soup.select("a.resultList-module__anchor__r8LvW")
    logging.info(f"Found {len(job_cards)} job cards in HTML.")

    jobs = []
    for card in job_cards:
        href = card.get("href", "")
        job_url = urljoin(BASE_URL, href)
        
        # Extract ID from URL ref param
        parsed_url = urlparse(job_url)
        params = parse_qs(parsed_url.query)
        job_id = "DEL_" + params.get("ref", [None])[0] if params.get("ref") else None

        title_el = card.select_one(".resultList-module__job__info_title__ejnCF")
        job_title = title_el.get_text(strip=True) if title_el else "Unknown Title"

        details = card.select("span.resultList-module__details__item__g77ck")
        # 0: Location, 1: Activity, 2: Contract
        location_raw = details[0].get_text(strip=True) if len(details) > 0 else None
        activity = details[1].get_text(strip=True) if len(details) > 1 else None
        contract_type = details[2].get_text(strip=True) if len(details) > 2 else None

        # Clean location
        location = None
        if location_raw:
            city = normalize_city(location_raw)
            # Inférer le pays depuis la ville si connue hors France (Tunis, Douala, Abidjan, etc.)
            country_from_city = get_country_from_city(city) if city else None
            if country_from_city:
                country = normalize_country(country_from_city)
            else:
                country = normalize_country("France")  # Deloitte FR context par défaut

            # Si la ville est None ou égale au pays, afficher uniquement le pays
            if not city or (country and city.lower() == country.lower()):
                location = country
            else:
                location = f"{city} - {country}"

        # Exclure les contrats libéraux
        if contract_type and "libéral" in contract_type.lower():
            logging.info(f"Excluding liberal contract: {job_title} ({job_id})")
            continue
        
        jobs.append({
            "job_id": job_id,
            "job_title": job_title,
            "contract_type": contract_type,
            "location": location,
            "job_family": activity,
            "job_url": job_url,
            "company_name": "Deloitte",
            "status": "Live",
            "first_seen": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "last_updated": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        })

    return jobs

# =========================================================
# VALIDATE JOB DESCRIPTION
# =========================================================
def is_valid_job_description(description: str) -> bool:
    """
    Vérifie si la description est une vraie description d'offre d'emploi.
    Retourne False si la description est vide, trop courte, ou ne contient pas
    de contenu pertinent d'offre d'emploi (uniquement du texte générique).
    """
    if not description or len(description.strip()) < 200:
        return False
    
    text_lower = description.lower()
    
    # Texte générique souvent présent sur toutes les pages Deloitte
    generic_texts = [
        "tous nos postes sont ouverts au télétravail",
        "rejoindre deloitte, c'est dire oui à une expérience",
        "great place to work",
        "#isayyes",
        "à votre futur chez deloitte"
    ]
    
    # Si la description ne contient QUE du texte générique (sans contenu spécifique)
    # on vérifie si elle contient au moins 3 phrases génériques consécutives
    generic_count = sum(1 for generic in generic_texts if generic in text_lower)
    
    # Mots-clés typiques d'une vraie offre d'emploi avec contenu spécifique
    specific_keywords = [
        "mission", "rôle", "profil recherché", "vous serez", 
        "vous intégrez", "vous travaillerez", "compétences requises",
        "expérience requise", "diplôme", "formation", "responsabilités", 
        "activités", "tâches", "fonction", "équipe", "client", "projet",
        "vous réaliserez", "vous participerez", "vous contribuerez",
        "assistance à", "accompagnement", "mise en place", "développement"
    ]
    
    # Vérifier si au moins 3 mots-clés spécifiques sont présents
    specific_keyword_count = sum(1 for keyword in specific_keywords if keyword in text_lower)
    
    # Si beaucoup de texte générique mais peu de contenu spécifique, c'est suspect
    if generic_count >= 3 and specific_keyword_count < 3:
        return False
    
    # Si moins de 3 mots-clés spécifiques, probablement pas une vraie offre
    if specific_keyword_count < 3:
        return False
    
    return True

# =========================================================
# FETCH INDIVIDUAL JOB DETAILS (Experience Level & Clean Description)
# =========================================================
async def fetch_job_experience(context: BrowserContext, job: Dict, sem: asyncio.Semaphore):
    async with sem:
        page = await context.new_page()
        try:
            # Navigate and wait for the specific content container
            await page.goto(job["job_url"], timeout=config.PAGE_TIMEOUT, wait_until="domcontentloaded")
            
            # Wait for the actual job content to load (Deloitte uses dynamic rendering)
            try:
                await page.wait_for_selector(".deloitte-content-main-bloc", timeout=10000)
            except:
                logging.warning(f"Timeout waiting for content on {job['job_url']}")

            html = await page.content()
            soup = BeautifulSoup(html, "html.parser")
            
            # 0. Extract seniority/experience level from header (same level as contract and location)
            seniority = None
            seniority_text = None
            
            # Chercher dans la structure similaire à la liste (si présente sur la page de détail)
            # La structure peut être la même que dans get_all_jobs avec les spans de détails
            header_details_spans = soup.select("span.resultList-module__details__item__g77ck")
            
            # Chercher dans tous les spans de détails pour trouver la séniorité
            for span in header_details_spans:
                text = span.get_text(strip=True)
                text_lower = text.lower()
                # Chercher les mots-clés de séniorité
                if any(word in text_lower for word in ["étudiant", "jeune diplômé", "junior", "expérimenté", "senior", "confirmé", "stagiaire", "alternant"]):
                    seniority_text = text
                    break
            
            # Si pas trouvé, chercher dans toute la zone header de la page
            if not seniority_text:
                # Chercher dans différentes zones possibles du header
                header_selectors = [
                    ".job-header", ".offer-header", ".job-detail-header", 
                    "header[class*='job']", ".job-meta", ".offer-meta",
                    ".deloitte-content-header", ".content-header"
                ]
                for selector in header_selectors:
                    header_area = soup.select_one(selector)
                    if header_area:
                        header_text = header_area.get_text(" ", strip=True).lower()
                        # Chercher les mots-clés
                        keywords_map = {
                            "étudiant": "Étudiant",
                            "jeune diplômé": "Jeune diplômé",
                            "junior": "Junior",
                            "expérimenté": "Expérimenté",
                            "senior": "Senior",
                            "confirmé": "Confirmé"
                        }
                        for keyword_lower, keyword_original in keywords_map.items():
                            if keyword_lower in header_text:
                                seniority_text = keyword_original
                                break
                        if seniority_text:
                            break
            
            # Mapper la séniorité aux valeurs standardisées
            if seniority_text:
                seniority_lower = seniority_text.lower()
                if any(word in seniority_lower for word in ["étudiant", "jeune diplômé", "junior", "stagiaire", "alternant"]):
                    seniority = "0 - 2 ans"
                elif "expérimenté" in seniority_lower or "confirmé" in seniority_lower:
                    seniority = "3 - 5 ans"
                elif "senior" in seniority_lower:
                    # Vérifier dans le titre pour affiner
                    title_lower = job.get("job_title", "").lower()
                    if "senior manager" in title_lower or "director" in title_lower or "manager senior" in title_lower:
                        seniority = "11 ans et plus"
                    else:
                        seniority = "6 - 10 ans"
            
            # Si c'est un stage ou alternance dans le contrat, forcer 0-2 ans
            if job.get("contract_type"):
                contract_lower = job["contract_type"].lower()
                if any(word in contract_lower for word in ["stage", "alternance", "apprentissage"]):
                    seniority = "0 - 2 ans"
            
            # 1. Extract clean description from specific blocks
            description_blocks = soup.select(".deloitte-content-main-bloc .deloitte-content-bloc")
            if description_blocks:
                description_text = "\n\n".join([b.get_text(separator="\n", strip=True) for b in description_blocks])
            else:
                # Fallback to a broader but still scoped container if blocs are missing
                main_container = soup.select_one(".deloitte-content-main-bloc")
                description_text = main_container.get_text(separator="\n", strip=True) if main_container else ""

            # 2. Extract Experience and Education from the full page text (scoped if possible)
            text_lower = description_text.lower()

            # Experience level mapping - utiliser d'abord la séniorité extraite de l'en-tête
            experience_level = seniority  # Utiliser la séniorité extraite de l'en-tête si disponible
            
            # Si pas de séniorité trouvée dans l'en-tête, chercher dans le texte de la description
            if not experience_level:
                if "jeune diplômé" in text_lower or "stagiaire" in text_lower or "alternant" in text_lower:
                    experience_level = "0 - 2 ans"
                elif re.search(r'(\d+)\s*ans\s*d\'expérience', text_lower):
                    years_match = re.search(r'(\d+)\s*ans', text_lower)
                    if years_match:
                        years = int(years_match.group(1))
                        if years <= 2: experience_level = "0 - 2 ans"
                        elif years <= 5: experience_level = "3 - 5 ans"
                        elif years <= 10: experience_level = "6 - 10 ans"
                        else: experience_level = "11 ans et plus"
            
            # Education level mapping
            education_level = None
            if "bac + 5" in text_lower or "master" in text_lower or "école d'ingénieur" in text_lower or "école de commerce" in text_lower:
                education_level = "Bac + 5 / M2 et plus"
            elif "bac + 3" in text_lower or "licence" in text_lower:
                education_level = "Bac + 3 / L3"

            # 3. Validate and clean description
            # Vérifier si c'est une vraie description d'offre d'emploi
            if not is_valid_job_description(description_text):
                logging.warning(f"Invalid job description detected for {job['job_id']} - {job['job_title']}. Clearing description.")
                final_description = ""
            else:
                # Keep description length reasonable for CSV and replace newlines with spaces
                # to avoid CSV parsing issues in JavaScript
                cleaned_description = description_text.replace('\r\n', ' ').replace('\n', ' ').replace('\r', ' ')
                # Replace multiple spaces with single space
                cleaned_description = re.sub(r'\s+', ' ', cleaned_description)
                # Texte complet pour recherche par mots-clés (non affiché sur les vignettes), max 25k car.
                final_description = cleaned_description[:25000] if cleaned_description else ""
            
            job.update({
                "experience_level": experience_level,
                "education_level": education_level,
                "job_description": final_description,
                "job_family": classify_job_family(job["job_title"], final_description) if final_description else job.get("job_family")
            })

        except Exception as e:
            logging.warning(f"Failed to fetch details for {job['job_url']}: {e}")
        finally:
            await page.close()

# =========================================================
# MAIN
# =========================================================
async def main():
    start = time.time()
    logging.info("=" * 80)
    logging.info("DÉBUT PIPELINE DELOITTE JOB SCRAPER")
    logging.info("=" * 80)

    # Initialiser la base de données
    db = JobDatabase(config.DB_PATH)
    logging.info(f"Base de données initialisée: {config.DB_PATH}")

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=config.HEADLESS)
        context = await browser.new_context()

        # Block resources for performance
        await context.route("**/*", lambda route: route.abort() if route.request.resource_type in config.BLOCK_RESOURCES else route.continue_())

        # Étape 1: Collecter tous les jobs (avec leurs URLs)
        logging.info("\n📋 ÉTAPE 1: Collection des offres")
        jobs = await get_all_jobs(context)
        logging.info(f"Collected {len(jobs)} basic job listings.")

        # Extraire les URLs actuels
        all_current_links = {job['job_url'] for job in jobs if job.get('job_url')}

        # Étape 2: Identifier les nouveaux et les expirés
        logging.info("\n🔍 ÉTAPE 2: Analyse des changements")
        existing_live_urls = db.get_live_urls()

        new_urls = all_current_links - existing_live_urls
        expired_urls = existing_live_urls - all_current_links

        logging.info(f"✅ Nouvelles offres: {len(new_urls)}")
        logging.info(f"❌ Offres expirées: {len(expired_urls)}")

        # Étape 3: Marquer les expirées
        if expired_urls:
            logging.info("\n⏳ ÉTAPE 3: Marquage des offres expirées")
            db.mark_as_expired(expired_urls)
            logging.info(f"✓ {len(expired_urls)} offres marquées comme expirées")

        # Étape 4: Scraper les détails des nouveaux jobs
        if jobs:
            # Filtrer pour ne scraper que les nouveaux
            new_jobs = [job for job in jobs if job.get('job_url') in new_urls]
            
            if new_jobs:
                logging.info(f"\n🚀 ÉTAPE 4: Scraping de {len(new_jobs)} nouvelles offres")
                sem = asyncio.Semaphore(config.MAX_CONCURRENT_PAGES)
                tasks = [fetch_job_experience(context, job, sem) for job in new_jobs]
                
                for coro in tqdm(asyncio.as_completed(tasks), total=len(tasks), desc="Scraping experience levels"):
                    await coro
                
                # Insérer/mettre à jour les jobs dans la base
                # Deloitte n'affiche pas la date de parution : on utilise la date du premier scrape.
                # Si l'offre existe déjà (ex: revenue après expiration), on garde sa date.
                today = datetime.now().strftime('%Y-%m-%d')
                for job in new_jobs:
                    existing_date = db.get_existing_publication_date(job.get('job_url') or '')
                    job['publication_date'] = existing_date if existing_date else today
                    db.insert_or_update_job(job)
            else:
                logging.info("\n✓ Aucune nouvelle offre à scraper")

        await context.close()
        await browser.close()

    # Étape 5: Export CSV
    logging.info("\n💾 ÉTAPE 5: Export vers CSV")
    db.export_to_csv(config.CSV_PATH)
    logging.info(f"✓ CSV exporté: {config.CSV_PATH}")

    # Statistiques finales
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
        logging.info(f"  └─ Invalid (pages 404): {stats[3]}")
        logging.info("=" * 60)

    elapsed = time.time() - start
    logging.info(f"Time elapsed: {elapsed:.2f}s")
    logging.info("=" * 80)

if __name__ == "__main__":
    asyncio.run(main())
