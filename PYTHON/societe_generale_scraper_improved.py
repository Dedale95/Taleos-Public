#!/usr/bin/env python3
"""
SOCIÉTÉ GÉNÉRALE - JOB SCRAPER AMÉLIORÉ
Extrait TOUTES les données: dates, description, compétences, etc.
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
from urllib.parse import urljoin
from datetime import datetime
from city_normalizer import normalize_city
from country_normalizer import normalize_country
from job_family_classifier import classify_job_family
from experience_extractor import extract_experience_level

# ================= Logging =================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# ================= Constants =================
BASE_URL = "https://careers.societegenerale.com"
SEARCH_URL = f"{BASE_URL}/rechercher?query="

# ================= Config =================
class Config:
    MAX_CONCURRENT_PAGES = 5  # Réduit de 15 à 5 pour éviter les timeouts
    PAGE_TIMEOUT = 30000  # Augmenté de 20s à 30s
    WAIT_TIMEOUT = 10000
    MAX_RETRIES = 2
    HEADLESS = True
    BASE_DIR = Path(__file__).parent
    DB_PATH = BASE_DIR / "societe_generale_jobs.db"
    CSV_PATH = BASE_DIR / "societe_generale_jobs_improved.csv"

    BLOCK_RESOURCES = {
        "image", "stylesheet", "font", "media", "texttrack",
        "object", "beacon", "csp_report", "imageset"
    }

config = Config()

# =========================================================
# DATABASE MANAGER
# =========================================================
class JobDatabase:
    """Gestion de la base de données SQLite pour Société Générale"""

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

    def mark_error_pages_invalid(self) -> int:
        """Marque is_valid=0 pour les jobs dont le titre indique une page 404. Retourne le nombre mis à jour."""
        error_patterns = ['page not found', 'page introuvable', 'pagenot found']
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                "SELECT job_url, job_title FROM jobs WHERE is_valid = 1 AND job_title IS NOT NULL"
            )
            to_invalidate = [
                row[0] for row in cursor.fetchall()
                if row[1] and any(p in row[1].lower() for p in error_patterns)
            ]
            if to_invalidate:
                placeholders = ','.join('?' * len(to_invalidate))
                conn.execute(f"""
                    UPDATE jobs SET is_valid = 0, last_updated = CURRENT_TIMESTAMP
                    WHERE job_url IN ({placeholders})
                """, tuple(to_invalidate))
                conn.commit()
        return len(to_invalidate)

    def insert_or_update_job(self, job: Dict):
        """Insert ou update un job"""
        with sqlite3.connect(self.db_path) as conn:
            # Vérifier si le job a du contenu valide
            is_valid = 1 if (job.get('job_id') or job.get('job_title') or job.get('job_description')) else 0
            # Exclure les pages d'erreur 404 (tuiles vides "PageNot found", "Page introuvable")
            title_lower = (job.get('job_title') or '').lower()
            if any(err in title_lower for err in ['page not found', 'page introuvable', 'pagenot found']):
                is_valid = 0

            # Convertir les listes en JSON si nécessaire
            technical_skills = job.get('technical_skills', '[]')
            behavioral_skills = job.get('behavioral_skills', '[]')
            
            # Si c'est déjà une string, essayer de la parser
            if isinstance(technical_skills, str) and technical_skills.startswith('['):
                try:
                    technical_skills = json.dumps(eval(technical_skills), ensure_ascii=False)
                except:
                    technical_skills = json.dumps([], ensure_ascii=False)
            elif isinstance(technical_skills, list):
                technical_skills = json.dumps(technical_skills, ensure_ascii=False)
            else:
                technical_skills = json.dumps([], ensure_ascii=False)
            
            if isinstance(behavioral_skills, str) and behavioral_skills.startswith('['):
                try:
                    behavioral_skills = json.dumps(eval(behavioral_skills), ensure_ascii=False)
                except:
                    behavioral_skills = json.dumps([], ensure_ascii=False)
            elif isinstance(behavioral_skills, list):
                behavioral_skills = json.dumps(behavioral_skills, ensure_ascii=False)
            else:
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
                    publication_date = excluded.publication_date,
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

            # Convertir JSON strings en listes lisibles
            for col in ['technical_skills', 'behavioral_skills']:
                if col in df.columns:
                    df[col] = df[col].apply(lambda x: ', '.join(json.loads(x)) if x and x.startswith('[') else x)

            df.to_csv(csv_path, index=False, encoding='utf-8')

# =========================================================
# UTILITY: CLEAN DATE
# =========================================================
def clean_date(date_str: Optional[str]) -> Optional[str]:
    """
    Nettoie les dates du format 'Date de publication12/12/2025' 
    vers '2025-12-12'
    """
    if not date_str:
        return None
    
    # Supprimer les labels
    date_str = re.sub(r'(Date de publication|Publication date|Date de début|Start date)', '', date_str, flags=re.IGNORECASE)
    date_str = date_str.strip()
    
    # Si c'est "Immediately" ou équivalent, garder tel quel
    if date_str.lower() in ['immediately', 'immédiatement', 'asap']:
        return date_str
    
    # Parser la date DD/MM/YYYY ou YYYY/MM/DD
    patterns = [
        (r'(\d{2})/(\d{2})/(\d{4})', '{2}-{1}-{0}'),  # DD/MM/YYYY -> YYYY-MM-DD
        (r'(\d{4})/(\d{2})/(\d{2})', '{0}-{1}-{2}'),  # YYYY/MM/DD -> YYYY-MM-DD
    ]
    
    for pattern, format_str in patterns:
        match = re.search(pattern, date_str)
        if match:
            return format_str.format(*match.groups())
    
    return date_str

# =========================================================
# PAGE COUNT
# =========================================================
# IMPORTANT: La liste d'offres et la pagination sont rendues en JavaScript.
# Ne PAS supprimer le wait_for_selector ci-dessous : sans lui on ne récupère
# qu'une seule page (~10 URLs) et tout le reste est marqué Expiré à tort.
async def get_total_pages(context: BrowserContext) -> int:
    page = await context.new_page()
    await page.goto(SEARCH_URL, timeout=config.PAGE_TIMEOUT, wait_until="domcontentloaded")

    try:
        await page.click("#didomi-notice-disagree-button", timeout=3000)
        logging.info("Cookie banner closed")
    except:
        logging.info("Cookie banner not found")

    # Attendre que la pagination ou la liste d'offres soit rendue (JS)
    try:
        await page.wait_for_selector(
            'a.js-pager[title="Aller à la dernière page"], a[href*="/offres-d-emploi/"], a[href*="/en/job-offers/"]',
            timeout=config.WAIT_TIMEOUT
        )
    except Exception as e:
        logging.warning(f"Timeout waiting for job list / pagination: {e}")
    await asyncio.sleep(2)  # laisser le temps au dernier numéro de page d'apparaître

    html = await page.content()
    soup = BeautifulSoup(html, "html.parser")

    last_page = soup.select_one('a.js-pager[title="Aller à la dernière page"]')
    await page.close()

    return int(last_page["data-page"]) if last_page else 1

# =========================================================
# COLLECT JOB URLS
# =========================================================
async def fetch_urls(context: BrowserContext, page_num: int, sem: asyncio.Semaphore) -> List[str]:
    async with sem:
        page = await context.new_page()
        try:
            url = f"{SEARCH_URL}&page={page_num}"
            await page.goto(url, timeout=config.PAGE_TIMEOUT, wait_until="domcontentloaded")
            
            # Attendre que les liens offres soient rendus (JS)
            try:
                await page.wait_for_selector(
                    'a[href*="/offres-d-emploi/"], a[href*="/en/job-offers/"]',
                    timeout=config.WAIT_TIMEOUT
                )
            except Exception:
                pass
            await asyncio.sleep(2)
            
            html = await page.content()
            soup = BeautifulSoup(html, "html.parser")

            urls = set()
            for link in soup.select("a[href*='/offres-d-emploi/'], a[href*='/en/job-offers/']"):
                href = link.get("href", "")
                # Filter out saved jobs and other non-job pages
                if '/offres-sauvegardees' in href or '/jobs-etudiants' in href:
                    continue
                full_url = urljoin(BASE_URL, href)
                # Only keep URLs with job IDs (contains pattern like 25000XXX)
                if re.search(r'[0-9]{5}[A-Z0-9]{2,4}-(?:fr|en)', full_url):
                    urls.add(full_url)
            return list(urls)
        except Exception as e:
            logging.error(f"❌ Page {page_num} failed: {e}")
            return []
        finally:
            await page.close()

# =========================================================
# EXTRACT JOB DETAILS (IMPROVED)
# =========================================================
async def fetch_job_details(context: BrowserContext, url: str, sem: asyncio.Semaphore) -> Dict:
    async with sem:
        page = await context.new_page()
        try:
            await page.goto(url, timeout=config.PAGE_TIMEOUT, wait_until="domcontentloaded")
            await asyncio.sleep(1)  # Wait for JS rendering
            
            html = await page.content()
            soup = BeautifulSoup(html, "html.parser")

            # Extract job_id from URL
            job_id_match = re.search(r'-([A-Z0-9]+)-(?:fr|en)', url)
            job_id = "SG_" + job_id_match.group(1) if job_id_match else None

            # Title
            title_tag = soup.select_one("h1")
            job_title = title_tag.get_text(strip=True) if title_tag else None

            # Contract type (from badge) with harmonization
            contract_type = None
            for badge in soup.select("span.flex.pb-px"):
                text = badge.get_text(strip=True)
                # Harmonization mapping to match Crédit Agricole terminology
                contract_mapping = {
                    "Permanent contract": "CDI",
                    "Temporary contract": "CDD",
                    "Fixed term contract": "CDD",
                    "Internship": "Stage",
                    "Trainee": "Stage",
                    "International Volunteer Program": "VIE",
                    "V.I.E": "VIE",
                    "Graduate program": "Graduate Program",
                    "Alternance": "Alternance / Apprentissage",
                    # Keep these as-is
                    "CDI": "CDI",
                    "CDD": "CDD",
                    "Stage": "Stage",
                    "VIE": "VIE"
                }
                
                if text in contract_mapping:
                    contract_type = contract_mapping[text]
                    break

            # Location (harmonize to match CA format: "Ville - Pays")
            location_tag = soup.select_one("div.mask-location-check")
            location_raw = location_tag.get_text(strip=True) if location_tag else None
            
            # Transform "Ville, Région, Pays" or "Ville, Pays" to "Ville - Pays"
            # AND normalize city/country names
            location = None
            if location_raw:
                parts = [p.strip() for p in location_raw.split(',')]
                city_raw = None
                country_raw = None

                if len(parts) >= 2:
                    city_raw = parts[0]
                    country_raw = parts[-1]
                elif len(parts) == 1:
                    # Si une seule partie, c'est probablement un pays
                    country_raw = parts[0]
                    city_raw = None  # Pas de ville

                # Mapping ville → état pour harmonisation (US principalement)
                # Exemple: "Jersey City" → "New Jersey" pour cohérence
                city_to_state_mapping = {
                    "jersey city": "New Jersey",
                    "newark": "New Jersey",
                    "trenton": "New Jersey",
                    "new york city": "New York",
                    "new york": "New York",
                    "brooklyn": "New York",
                    "manhattan": "New York",
                    "boston": "Massachusetts",
                    "chicago": "Illinois",
                    "los angeles": "California",
                    "san francisco": "California",
                    "miami": "Florida",
                    "dallas": "Texas",
                    "houston": "Texas",
                    "austin": "Texas",
                    "seattle": "Washington",
                    "atlanta": "Georgia",
                }
                
                # Appliquer le mapping si la ville est reconnue
                if city_raw and city_raw.lower() in city_to_state_mapping:
                    city_raw = city_to_state_mapping[city_raw.lower()]

                # IMPORTANT : Vérifier si city_raw est en fait un pays
                # Liste des pays connus (à compléter)
                known_countries = {
                    'france', 'inde', 'india', 'japon', 'japan', 'pologne', 'poland', 
                    'roumanie', 'romania', 'chine', 'china', 'italie', 'italy', 
                    'allemagne', 'germany', 'espagne', 'spain', 'portugal', 
                    'belgique', 'belgium', 'suisse', 'switzerland', 'luxembourg',
                    'pays-bas', 'netherlands', 'royaume-uni', 'united kingdom', 
                    'états-unis', 'united states', 'usa', 'canada', 'singapour', 
                    'singapore', 'hong-kong', 'hong kong', 'australie', 'australia',
                    'grèce', 'greece', 'turquie', 'turkey', 'maroc', 'morocco'
                }
                
                # Si city_raw est un pays, le rejeter et utiliser comme pays si country_raw est vide
                if city_raw and city_raw.lower() in known_countries:
                    if not country_raw:
                        country_raw = city_raw
                    city_raw = None  # Rejeter la "ville" qui est en fait un pays

                city_normalized = normalize_city(city_raw) if city_raw else None
                country_normalized = normalize_country(country_raw) if country_raw else None

                # Si la ville est None (rejetée par le normaliseur) ou égale au pays, afficher uniquement le pays
                if city_normalized and country_normalized:
                    # Vérifier si la ville = pays
                    if city_normalized.lower() == country_normalized.lower():
                        location = country_normalized
                    else:
                        location = f"{city_normalized} - {country_normalized}"
                elif country_normalized:
                    location = country_normalized
                else:
                    location = None  # Si ni ville ni pays valides, location = None


            # Extract dates using regex
            def extract_date(label_patterns):
                text = soup.get_text()
                for pattern in label_patterns:
                    match = re.search(rf'{pattern}\s*:?\s*([^\n,]+)', text, re.IGNORECASE)
                    if match:
                        return clean_date(match.group(1).strip())
                return None

            publication_date = extract_date(["Date de publication", "Publication date"])
            start_date = extract_date(["Date de début", "Start date"])
            
            # Extract education level
            education_level = None
            text_lower = soup.get_text().lower()
            
            # Education mapping to match CA format
            education_patterns = [
                (r'bac\s*\+\s*5|master|mba|phd|doctorat|ingénieur|engineer|grande école', "Bac + 5 / M2 et plus"),
                (r'bac\s*\+\s*4|m1', "Bac + 4 / M1"),
                (r'bac\s*\+\s*3|bachelor|licence|l3', "Bac + 3 / L3"),
                (r'bac\s*\+\s*2|l2|bts|dut', "Bac + 2 / L2"),
                (r'\bbac\b(?!\s*\+)', "Bac"),
            ]
            
            for pattern, level in education_patterns:
                if re.search(pattern, text_lower):
                    education_level = level
                    break
            
            # Extract experience level (module partagé)
            experience_level = extract_experience_level(soup.get_text(), contract_type, job_title)

            # Extract sections for description and skills
            description_parts = []
            technical_skills = []
            behavioral_skills = []
            
            # Get all text sections (pour description + texte complet recherche mots-clés)
            all_sections = soup.select("section, div[class*='section'], div.wysiwyg")
            all_content_parts = []  # tout le texte pour job_description (recherche)

            for section in all_sections:
                h = section.select_one("h2, h3, h4, h5")
                section_title = h.get_text(strip=True).lower() if h else ""
                content = section.get_text(strip=True)
                if content:
                    all_content_parts.append(content)

                # Description (pour job_family etc.)
                if any(kw in section_title for kw in ['mission', 'description', 'poste', 'role', 'responsabilit', 'quotidien']):
                    description_parts.append(content)

                # Skills
                if any(kw in section_title for kw in ['compétence', 'skill', 'profil', 'requirement', 'qualification', 'exigence', 'vous', 'qualifications']):
                    # Try to extract bullet points
                    bullets = section.select("li, p")
                    if bullets:
                        for item in bullets:
                            text = item.get_text(strip=True)
                            if len(text) < 10 or len(text) > 300:  # Skip too short/long
                                continue
                            # Simple heuristic: if it contains tech keywords, it's technical
                            if any(tech in text.lower() for tech in ['python', 'java', 'sql', 'model', 'data', 'system', 'software', 'excel', 'vba', 'c++', 'risk', 'quantitative', 'financial', 'analytics']):
                                technical_skills.append(text)
                            elif any(soft in text.lower() for soft in ['communication', 'teamwork', 'leadership', 'collaboration', 'autonome', 'rigoureux', 'organizational']):
                                behavioral_skills.append(text)

            # Texte complet pour la recherche par mots-clés (toutes les sections, pas affiché sur les vignettes)
            job_description = " ".join(all_content_parts)[:25000] if all_content_parts else None
            if not job_description:
                job_description = soup.get_text(separator=" ", strip=True)[:25000]
            
            # Classify job family based on title and description
            job_family = classify_job_family(job_title or "", job_description or "")
            
            # If no skills found, try to extract from full text
            if not technical_skills and not behavioral_skills:
                full_text = soup.get_text()
                # Look for common skill patterns
                skill_section_match = re.search(r'(?:Compétences|Skills|Qualifications)[:\s]+(.*?)(?:Pourquoi|Why|Avantages|\Z)', full_text, re.DOTALL | re.IGNORECASE)
                if skill_section_match:
                    skills_text = skill_section_match.group(1)
                    # Split by newlines or common separators
                    potential_skills = re.split(r'[\n•\-]', skills_text)
                    for skill in potential_skills[:10]:  # Limit to 10
                        skill = skill.strip()
                        if 20 < len(skill) < 200:
                            technical_skills.append(skill)
            
            return {
                "job_id": job_id,
                "job_title": job_title,
                "contract_type": contract_type,
                "publication_date": publication_date,
                "location": location,
                "job_family": job_family,
                "duration": None,  # Not clearly available
                "management_position": None,  # Not clearly available
                "status": "Live",  # Assume live if we can access it
                "education_level": education_level,
                "experience_level": experience_level,
                "training_specialization": None,
                "technical_skills": str(technical_skills) if technical_skills else "[]",
                "behavioral_skills": str(behavioral_skills) if behavioral_skills else "[]",
                "tools": None,
                "languages": None,
                "job_description": job_description,
                "company_name": "Société Générale",
                "company_description": None,
                "job_url": url,
                "first_seen": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "last_updated": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }
            
        except Exception as e:
            logging.warning(f"Job failed: {url} ({e})")
            return {
                "job_id": None, "job_title": None, "contract_type": None,
                "publication_date": None, "location": None, "job_family": None,
                "duration": None, "management_position": None, "status": None,
                "education_level": None, "experience_level": None,
                "training_specialization": None, "technical_skills": "[]",
                "behavioral_skills": "[]", "tools": None, "languages": None,
                "job_description": None, "company_name": "Société Générale",
                "company_description": None, "job_url": url,
                "first_seen": None, "last_updated": None
            }
        finally:
            await page.close()

# =========================================================
# MAIN PIPELINE
# =========================================================
async def main():
    start = time.time()
    logging.info("=" * 80)
    logging.info("DÉBUT PIPELINE SOCIÉTÉ GÉNÉRALE JOB SCRAPER (AMÉLIORÉ)")
    logging.info("=" * 80)

    # Initialiser la base de données
    db = JobDatabase(config.DB_PATH)
    logging.info(f"Base de données initialisée: {config.DB_PATH}")

    # Nettoyer les tuiles d'erreur 404 déjà en base
    n_invalid = db.mark_error_pages_invalid()
    if n_invalid:
        logging.info(f"🧹 {n_invalid} offres 'Page not found' marquées comme invalides")

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=config.HEADLESS)
        context = await browser.new_context()

        await context.route(
            "**/*",
            lambda route: route.abort()
            if route.request.resource_type in config.BLOCK_RESOURCES
            else route.continue_()
        )

        # Étape 1: Collecter tous les liens
        logging.info("\n📋 ÉTAPE 1: Collection des liens d'offres")
        total_pages = await get_total_pages(context)
        logging.info(f"Total pages detected: {total_pages}")

        sem_pages = asyncio.Semaphore(config.MAX_CONCURRENT_PAGES)
        page_tasks = [fetch_urls(context, p, sem_pages) for p in range(1, total_pages + 1)]

        all_current_links = set()
        for coro in tqdm(asyncio.as_completed(page_tasks), total=total_pages, desc="Collecting URLs"):
            all_current_links.update(await coro)

        logging.info(f"Total job URLs collected: {len(all_current_links)}")

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

        # Étape 4: Scraper les nouveaux détails
        if new_urls:
            logging.info(f"\n🚀 ÉTAPE 4: Scraping de {len(new_urls)} nouvelles offres")
            sem_jobs = asyncio.Semaphore(config.MAX_CONCURRENT_PAGES)
            job_tasks = [fetch_job_details(context, url, sem_jobs) for url in new_urls]

            results = []
            for coro in tqdm(asyncio.as_completed(job_tasks), total=len(job_tasks), desc="Scraping jobs"):
                job_data = await coro
                if job_data:
                    results.append(job_data)
                    db.insert_or_update_job(job_data)
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
