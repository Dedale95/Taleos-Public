#!/usr/bin/env python3
"""
CRÉDIT MUTUEL - JOB SCRAPER
Extrait les offres d'emploi depuis recrutement.creditmutuel.fr
Utilise Playwright pour le chargement progressif (Afficher plus) + requests pour les détails.
Tags: famille de métier, expérience, localisation, type de contrat
"""

import asyncio
import logging
import re
import sqlite3
import json
from pathlib import Path
from typing import List, Dict, Set, Optional
from playwright.async_api import async_playwright
from bs4 import BeautifulSoup
from tqdm.asyncio import tqdm
from urllib.parse import urljoin
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

try:
    from city_normalizer import normalize_city
    from country_normalizer import normalize_country, get_country_from_city
    from job_family_classifier import classify_job_family
    from experience_extractor import extract_experience_level
    from credit_mutuel_job_family_mapping import map_credit_mutuel_family
except ImportError:
    import sys
    sys.path.append(str(Path(__file__).parent))
    from city_normalizer import normalize_city
    from country_normalizer import normalize_country, get_country_from_city
    from job_family_classifier import classify_job_family
    from experience_extractor import extract_experience_level
    from credit_mutuel_job_family_mapping import map_credit_mutuel_family

# ================= Logging =================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# ================= Constants =================
BASE_URL = "https://recrutement.creditmutuel.fr"
LISTING_URL = f"{BASE_URL}/fr/nos_offres.html"

# ================= Config =================
class Config:
    MAX_LOAD_MORE_ROUNDS = 70  # Clics sur "Afficher plus" (~15 offres/round)
    PAGE_TIMEOUT = 45000
    WAIT_AFTER_CLICK = 2
    HEADLESS = True
    BASE_DIR = Path(__file__).parent
    DB_PATH = BASE_DIR / "credit_mutuel_jobs.db"
    CSV_PATH = BASE_DIR / "credit_mutuel_jobs.csv"
    MAX_WORKERS = 10
    REQUEST_TIMEOUT = 30

config = Config()

# ================= Contract type mapping =================
CONTRACT_MAPPING = {
    "cdi": "CDI",
    "cdd": "CDD",
    "stage": "Stage",
    "vie": "VIE",
    "alternance": "Alternance",
    "apprentissage": "Alternance",
    "contrat de professionnalisation": "Alternance",
    "contrat étudiant": "Contrat étudiant",
    "auxiliaire de vacances": "Auxiliaire de vacances",
    "reconversion professionnelle (cdi)": "CDI",
}

# Experience mapping (CM raw → Taleos format)
EXPERIENCE_MAPPING = {
    "débutant": "0 - 2 ans",
    "debutant": "0 - 2 ans",
    "junior": "0 - 2 ans",
    "confirmé": "6 - 10 ans",
    "confirme": "6 - 10 ans",
    "senior": "11 ans et plus",
    "expert": "11 ans et plus",
}


def normalize_contract(raw: str) -> str:
    if not raw:
        return ""
    cleaned = re.sub(r'\s*\([^)]*\)\s*', '', raw).strip().lower()
    return CONTRACT_MAPPING.get(cleaned, raw.strip())


def normalize_experience(raw: str) -> Optional[str]:
    if not raw:
        return None
    key = str(raw).strip().lower()
    return EXPERIENCE_MAPPING.get(key)


def normalize_location_cm(location_raw: str) -> str:
    """
    CM format: "LILLE (59)" ou "STRASBOURG (67)" ou "LUXEMBOURG (Luxembourg)"
    Output: "Lille - France" ou "Luxembourg - Luxembourg"
    """
    if not location_raw or not str(location_raw).strip():
        return ""
    loc = str(location_raw).strip()
    # Format: VILLE (XX) où XX = code département ou pays
    match = re.match(r'^(.+?)\s*\(([^)]+)\)\s*$', loc)
    if match:
        city_raw = match.group(1).strip()
        code = match.group(2).strip()
        # Luxembourg comme pays
        if code.upper() == "LUXEMBOURG":
            return f"{normalize_city(city_raw) or city_raw} - Luxembourg"
        # Code département français (01-95, 2A, 2B, 971-976)
        if re.match(r'^(0[1-9]|[1-8]\d|9[0-5]|2[AB]|97[1-6])$', code, re.I):
            city = normalize_city(city_raw) or city_raw
            return f"{city} - France"
    # Fallback: utiliser tel quel avec France
    city = normalize_city(loc) or loc
    return f"{city} - France"


# =========================================================
# DATABASE
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

    def mark_as_expired(self, urls: Set[str]):
        if not urls:
            return
        with sqlite3.connect(self.db_path) as conn:
            placeholders = ','.join('?' * len(urls))
            conn.execute(f"""
                UPDATE jobs SET status = 'Expired', last_updated = CURRENT_TIMESTAMP
                WHERE job_url IN ({placeholders})
            """, tuple(urls))
            conn.commit()

    def insert_or_update_job(self, job: Dict):
        with sqlite3.connect(self.db_path) as conn:
            is_valid = 1 if (job.get('job_id') or job.get('job_title') or job.get('job_description')) else 0
            technical_skills = job.get('technical_skills') or '[]'
            behavioral_skills = job.get('behavioral_skills') or '[]'
            if isinstance(technical_skills, list):
                technical_skills = json.dumps(technical_skills, ensure_ascii=False)
            if isinstance(behavioral_skills, list):
                behavioral_skills = json.dumps(behavioral_skills, ensure_ascii=False)

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
                behavioral_skills, job.get('tools'), job.get('languages'),
                job.get('job_description'), job.get('company_name'),
                job.get('company_description'), is_valid
            ))
            conn.commit()


# =========================================================
# COLLECT JOB URLS (Playwright - load more)
# =========================================================
async def collect_all_job_urls() -> List[str]:
    """Collecte toutes les URLs d'offres en cliquant sur 'Afficher plus'."""
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=config.HEADLESS)
        page = await browser.new_page()
        await page.goto(LISTING_URL, timeout=config.PAGE_TIMEOUT, wait_until='domcontentloaded')
        await asyncio.sleep(3)

        # Fermer bannière cookies
        await page.evaluate("document.getElementById('cookieLB')?.remove()")
        await asyncio.sleep(0.5)

        all_ids = set()
        for round_num in range(config.MAX_LOAD_MORE_ROUNDS):
            # Compter les liens actuels
            ids = await page.evaluate("""
                () => {
                    const links = document.querySelectorAll('a[href*="offre.html?annonce="]');
                    const s = new Set();
                    links.forEach(a => {
                        const m = a.href.match(/annonce=(\\d+)/);
                        if (m) s.add(m[1]);
                    });
                    return Array.from(s);
                }
            """)
            prev_count = len(all_ids)
            all_ids.update(ids)

            if len(all_ids) == prev_count:
                logging.info(f"Plus de nouvelles offres après {round_num} clics")
                break

            # Scroll + clic Afficher plus
            await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            await asyncio.sleep(0.3)
            clicked = await page.evaluate("""
                () => {
                    const btns = Array.from(document.querySelectorAll('a.ei_btn'));
                    const loadMore = btns.find(a => a.textContent && a.textContent.includes('Afficher plus'));
                    if (loadMore) {
                        loadMore.scrollIntoView();
                        loadMore.click();
                        return true;
                    }
                    return false;
                }
            """)
            if not clicked:
                break
            await asyncio.sleep(config.WAIT_AFTER_CLICK)

        await browser.close()

        urls = [f"{BASE_URL}/fr/offre.html?annonce={aid}" for aid in all_ids]
        logging.info(f"Collecté {len(urls)} URLs d'offres")
        return urls


# =========================================================
# SCRAPE JOB DETAIL (requests + BeautifulSoup)
# =========================================================
def create_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
    })
    return session


def scrape_job_detail(url: str, session: requests.Session) -> Optional[Dict]:
    """Scrape le détail d'une offre (table avec Type contrat, Métier, Localisation, etc.)."""
    try:
        r = session.get(url, timeout=config.REQUEST_TIMEOUT)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")

        job = {
            "job_url": url,
            "job_id": "",
            "job_title": "",
            "contract_type": "",
            "publication_date": "",
            "location": "",
            "job_family": "",
            "duration": "",
            "management_position": "",
            "status": "Live",
            "education_level": "",
            "experience_level": "",
            "training_specialization": "",
            "technical_skills": [],
            "behavioral_skills": [],
            "tools": "",
            "languages": "",
            "job_description": "",
            "company_name": "Crédit Mutuel",
            "company_description": "",
        }

        # job_id depuis URL
        m = re.search(r'annonce=(\d+)', url)
        if m:
            job["job_id"] = f"CM_{m.group(1)}"

        # Titre depuis h1
        h1 = soup.select_one("h1")
        if h1:
            job["job_title"] = h1.get_text(strip=True)

        # Tableau détail : label -> value (3 colonnes)
        for row in soup.select("table tr"):
            cells = row.find_all(["td", "th"])
            if len(cells) >= 3:
                label = (cells[0].get_text(strip=True) or "").lower()
                value = (cells[2].get_text(strip=True) or "").strip()
                if not value:
                    continue
                if "type de contrat" in label or "contrat" in label:
                    job["contract_type"] = normalize_contract(value) or value
                elif "métier" in label:
                    job["job_family"] = map_credit_mutuel_family(value)
                elif "localisation" in label:
                    job["location"] = normalize_location_cm(value)
                elif "niveau d'études" in label or "niveau d études" in label:
                    job["education_level"] = value
                elif "niveau d'expérience" in label or "niveau d expérience" in label:
                    job["experience_level"] = normalize_experience(value) or value
                elif "date de publication" in label:
                    # DD/MM/YYYY -> YYYY-MM-DD
                    dm = re.search(r'(\d{2})/(\d{2})/(\d{4})', value)
                    if dm:
                        job["publication_date"] = f"{dm.group(3)}-{dm.group(2)}-{dm.group(1)}"
                    else:
                        job["publication_date"] = value

        # Description : texte principal (éviter le bloc diversité en début)
        main_content = soup.select_one("main, .content, article, [role='main']") or soup
        desc_parts = []
        for p in main_content.find_all(["p", "div"], recursive=True):
            text = p.get_text(strip=True)
            if text and len(text) > 50 and "diversité" not in text.lower()[:100]:
                if "mission" in text.lower() or "vous " in text.lower() or "nous " in text.lower():
                    desc_parts.append(text)
        if desc_parts:
            job["job_description"] = "\n\n".join(desc_parts[:15])
        if not job["job_description"]:
            job["job_description"] = main_content.get_text(separator="\n", strip=True)[:15000]

        # Fallback job_family si vide
        if not job["job_family"]:
            job["job_family"] = classify_job_family(
                job.get("job_title", ""),
                job.get("job_description", "")
            )

        # Fallback experience_level si vide
        if not job["experience_level"]:
            job["experience_level"] = extract_experience_level(
                job.get("job_description", ""),
                job.get("contract_type"),
                job.get("job_title")
            )

        return job
    except Exception as e:
        logging.warning(f"Erreur scraping {url}: {e}")
        return None


# =========================================================
# MAIN PIPELINE
# =========================================================
async def main_async():
    db = JobDatabase(config.DB_PATH)
    session = create_session()

    logging.info("=" * 60)
    logging.info("CRÉDIT MUTUEL - DÉBUT DU SCRAPING")
    logging.info("=" * 60)

    # 1. Collecter les URLs
    logging.info("Étape 1: Collecte des URLs...")
    urls = await collect_all_job_urls()
    if not urls:
        logging.error("Aucune URL collectée")
        return

    # 2. Déterminer les expirées
    existing_live = db.get_live_urls()
    current_set = set(urls)
    expired = existing_live - current_set
    if expired:
        db.mark_as_expired(expired)
        logging.info(f"Marquées expirées: {len(expired)}")

    # 3. Scraper les détails (parallèle avec requests)
    logging.info("Étape 2: Scraping des détails...")
    jobs = []
    with ThreadPoolExecutor(max_workers=config.MAX_WORKERS) as executor:
        futures = {executor.submit(scrape_job_detail, u, session): u for u in urls}
        for future in tqdm(as_completed(futures), total=len(futures), desc="Offres"):
            job = future.result()
            if job:
                jobs.append(job)

    # 4. Sauvegarder
    for job in jobs:
        db.insert_or_update_job(job)

    logging.info(f"Terminé: {len(jobs)} offres traitées")
    logging.info("=" * 60)


def main():
    asyncio.run(main_async())


if __name__ == "__main__":
    main()
