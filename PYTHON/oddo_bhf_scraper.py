#!/usr/bin/env python3
"""
ODDO BHF - JOB SCRAPER
Extrait les offres d'emploi depuis la plateforme Altays (recrutement.altays-progiciels.com/oddo)
Tags: familles de métier, expérience, localisation, type de contrat
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
    from oddo_bhf_job_family_mapping import map_oddo_bhf_family
except ImportError:
    import sys

    sys.path.append(str(Path(__file__).parent))
    from city_normalizer import normalize_city
    from country_normalizer import normalize_country, get_country_from_city
    from job_family_classifier import classify_job_family
    from experience_extractor import extract_experience_level
    from oddo_bhf_job_family_mapping import map_oddo_bhf_family

# ================= Logging =================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# ================= Constants =================
BASE_URL = "https://recrutement.altays-progiciels.com/oddo"
LISTING_URL = f"{BASE_URL}/fr/recherche.html"
OFFRES_URL = f"{BASE_URL}/fr/offres.html"

# ================= Config =================
class Config:
    MAX_CONCURRENT_PAGES = 5
    PAGE_TIMEOUT = 30000
    WAIT_TIMEOUT = 10000
    HEADLESS = True
    BASE_DIR = Path(__file__).parent
    DB_PATH = BASE_DIR / "oddo_bhf_jobs.db"
    CSV_PATH = BASE_DIR / "oddo_bhf_jobs.csv"

    BLOCK_RESOURCES = {
        "image", "font", "media", "texttrack",
        "object", "beacon", "csp_report", "imageset"
    }

    # Mapping type de contrat Altays → Taleos
    CONTRACT_MAPPING = {
        "cdi": "CDI",
        "permanent contract": "CDI",
        "cdd": "CDD",
        "fixed-term contract": "CDD",
        "stage": "Stage",
        "internship": "Stage",
        "internship/working student job": "Stage",
        "vie": "VIE",
        "alternance": "Alternance / Apprentissage",
        "dual studies": "Alternance / Apprentissage",
    }


config = Config()


# =========================================================
# DATABASE MANAGER
# =========================================================
class JobDatabase:
    """Gestion de la base de données SQLite pour ODDO BHF"""

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
            cursor = conn.execute(
                "SELECT job_url FROM jobs WHERE status = 'Live' AND is_valid = 1"
            )
            return {row[0] for row in cursor.fetchall()}

    def get_existing_publication_date(self, job_url: str) -> Optional[str]:
        """Récupère la date de publication existante (pour ne jamais l'écraser au re-scrape)"""
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
# NORMALIZE HELPERS
# =========================================================
def normalize_contract(raw: Optional[str]) -> Optional[str]:
    """Normalise le type de contrat Altays vers format Taleos"""
    if not raw:
        return None
    key = raw.strip().lower()
    return config.CONTRACT_MAPPING.get(key, raw.strip())


def build_location(city_raw: Optional[str], country_raw: Optional[str]) -> Optional[str]:
    """Construit la location au format 'Ville - Pays'"""
    city = normalize_city(city_raw) if city_raw else None
    country = normalize_country(country_raw) if country_raw else None

    if not country:
        return city or None
    if not city or (country and city.lower() == country.lower()):
        return country
    correct_country = get_country_from_city(city) if city else None
    if correct_country:
        country = normalize_country(correct_country)
    return f"{city} - {country}"


def parse_publication_date(text: str) -> Optional[str]:
    """Parse DD/MM/YYYY ou MM/DD/YYYY vers YYYY-MM-DD"""
    if not text or not text.strip():
        return None
    text = text.strip()
    # DD/MM/YYYY
    m = re.search(r'(\d{1,2})/(\d{1,2})/(\d{4})', text)
    if m:
        d, mo, y = m.group(1), m.group(2), m.group(3)
        return f"{y}-{mo.zfill(2)}-{d.zfill(2)}"
    return text


# =========================================================
# COLLECT ALL JOB URLS (paginated)
# =========================================================
async def collect_all_job_urls(context: BrowserContext) -> List[Dict]:
    """Collecte toutes les URLs d'offres depuis la liste paginée"""
    page = await context.new_page()
    all_jobs = []

    try:
        # Charger la page de recherche (sans filtre → toutes les offres)
        await page.goto(LISTING_URL, timeout=config.PAGE_TIMEOUT, wait_until="networkidle")
        await asyncio.sleep(2)

        page_num = 1
        while True:
            html = await page.content()
            soup = BeautifulSoup(html, "html.parser")

            # Extraire les offres de cette page
            for item in soup.select("li.jobs__detail"):
                link = item.select_one("a[href*='/offres/'][href*='.html']")
                if not link:
                    continue
                href = link.get("href", "")
                title = link.get_text(strip=True)
                if href and title and "postulation" not in href:
                    full_url = urljoin(BASE_URL + "/", href.split("?")[0])
                    # Extraire job_id depuis l'URL (dernier nombre)
                    match = re.search(r'-(\d+)\.html', href)
                    job_id = "ODDO_" + match.group(1) if match else None

                    all_jobs.append({
                        "job_url": full_url,
                        "job_id": job_id,
                        "job_title": title,
                    })

            # Chercher le bouton "Voir plus d'offres" / "Show more"
            show_more = await page.query_selector(
                'button:has-text("Voir plus"), button:has-text("Show more"), '
                'a:has-text("Voir plus"), [id*="show-more"], .jobs__more button'
            )
            if show_more:
                try:
                    await show_more.click(timeout=3000)
                    await asyncio.sleep(2)
                    page_num += 1
                    continue
                except Exception:
                    pass

            # Sinon essayer la pagination par URL
            next_url = f"{OFFRES_URL}?page={page_num + 1}"
            try:
                res = await page.goto(next_url, timeout=config.PAGE_TIMEOUT, wait_until="domcontentloaded")
                if res and res.status == 200:
                    await asyncio.sleep(2)
                    new_soup = BeautifulSoup(await page.content(), "html.parser")
                    new_items = new_soup.select("li.jobs__detail")
                    if not new_items:
                        break
                    page_num += 1
                    continue
            except Exception:
                pass
            break

        # Dédupliquer par URL
        seen = set()
        unique = []
        for j in all_jobs:
            if j["job_url"] not in seen:
                seen.add(j["job_url"])
                unique.append(j)

        logging.info(f"Collected {len(unique)} job URLs")
        return unique

    finally:
        await page.close()


# =========================================================
# FETCH JOB DETAIL
# =========================================================
async def fetch_job_detail(context: BrowserContext, job: Dict, sem: asyncio.Semaphore) -> Dict:
    """Récupère les détails d'une offre (contract, location, family, description, etc.)"""
    async with sem:
        page = await context.new_page()
        try:
            await page.goto(job["job_url"], timeout=config.PAGE_TIMEOUT, wait_until="domcontentloaded")
            await asyncio.sleep(1)

            html = await page.content()
            soup = BeautifulSoup(html, "html.parser")

            # Titre (déjà connu, peut être plus précis ici)
            title_el = soup.select_one("h1")
            job_title = title_el.get_text(strip=True) if title_el else job.get("job_title", "")

            # Référence (job_id)
            ref_el = soup.find(string=re.compile(r"Référence\s*:", re.I))
            ref = None
            if ref_el:
                parent = ref_el.parent
                if parent:
                    ref_text = parent.get_text(strip=True)
                    m = re.search(r"Référence\s*:\s*(\S+)", ref_text, re.I)
                    if m:
                        ref = "ODDO_" + m.group(1).strip()

            job_id = ref or job.get("job_id")

            # Type de contrat, pays, domaine (dans les blocs visibles)
            contract_type = None
            country_raw = None
            job_family_raw = None
            city_raw = None
            publication_date = None

            # Chercher dans les labels structure
            labels = soup.select("[class*='detail'], .job-detail, .offer-meta, dd, [class*='meta']")
            full_text = soup.get_text(separator=" ", strip=True).lower()

            # Patterns pour les métadonnées
            for block in soup.select("main article, .job-content, .offer-content, .content, [role='main']"):
                text = block.get_text(separator=" | ", strip=True)
                # Type de contrat
                for ct in ["CDI", "CDD", "Stage", "VIE", "Alternance"]:
                    if ct.lower() in text.lower() and not contract_type:
                        contract_type = ct
                        break
                if not contract_type:
                    if "permanent" in text.lower() or "cdi" in text.lower():
                        contract_type = "CDI"
                    elif "fixed-term" in text.lower() or "cdd" in text.lower():
                        contract_type = "CDD"
                    elif "stage" in text.lower() or "intern" in text.lower() or "praktikant" in text.lower() or "werkstudent" in text.lower():
                        contract_type = "Stage"
                    elif "vie" in text.lower():
                        contract_type = "VIE"
                    elif "alternance" in text.lower() or "dual studies" in text.lower():
                        contract_type = "Alternance / Apprentissage"

            # Priorité 1: "Localisation : PARIS" (label explicite sur la page détail)
            loc_match = re.search(r'Localisation\s*:\s*([A-Za-zÀ-ÿ\-]+)', full_text, re.IGNORECASE)
            if loc_match:
                loc_raw = loc_match.group(1).strip()
                if loc_raw and "domaine" not in loc_raw.lower() and "type" not in loc_raw.lower():
                    city_raw = loc_raw

            # Structure Altays : bloc "Lieu du poste" avec liste [ville, contrat, domaine, pays, date]
            # Ex: - Saarbrücken | - CDI | - Others | - Allemagne | - 04/03/2026
            known_contracts = {"CDI", "CDD", "Stage", "VIE", "Alternance"}
            known_domains = {"Asset Management", "Audit", "Compliance", "Corporate Banking", "Corporate Finance",
                            "Human Resources", "IT", "Legal", "Private Wealth Management", "Others", "Risques",
                            "Communication/Marketing", "Operations/Account Safekeeping and custodial services",
                            "Equities and Fixed Income", "Credit Risk Management", "Corporate Services", "Innovation",
                            "International Banking", "Metals Trading", "Facilities Management", "Foreign Exchange & Funds",
                            "Independent Financial Advisors", "Private Equity", "Transformation"}
            known_countries = {"France", "Allemagne", "Germany", "Suisse", "Switzerland", "Luxembourg"}

            meta_block = soup.find(string=re.compile(r"Lieu du poste|Date de première publication", re.I))
            if meta_block:
                parent = meta_block.find_parent(["div", "section", "article", "main"])
                if parent:
                    meta_items = [li.get_text(strip=True) for li in parent.select("ul li") if li.get_text(strip=True)]
                    # Villes connues à privilégier (éviter les adresses type "12 boulevard...")
                    known_cities_oddo = {"PARIS", "Paris", "Saarbrücken", "Frankfurt", "Francfort", "Luxembourg", "Luxembourg Ville", "Düsseldorf", "Milan", "Genève", "Zurich"}
                    for item in meta_items:
                        if re.match(r"\d{1,2}/\d{1,2}/\d{4}", item):
                            publication_date = parse_publication_date(item)
                        elif item in known_contracts:
                            contract_type = item
                        elif item in known_countries:
                            country_raw = item
                        elif item in known_domains:
                            job_family_raw = item
                        elif not city_raw and item and len(item) < 50 and "postuler" not in item.lower() and not re.match(r"^\d", item):
                            # Privilégier les villes connues ; éviter les adresses (boulevard, rue, allée)
                            if item in known_cities_oddo or (normalize_city(item) and "boulevard" not in item.lower() and "rue" not in item.lower() and "allée" not in item.lower() and "allee" not in item.lower()):
                                city_raw = item

            # Fallback: chercher les valeurs connues dans le HTML
            if not job_family_raw:
                domaines = ["Asset Management", "Audit", "Compliance", "Corporate Banking", "Corporate Finance",
                           "Human Resources", "IT", "Legal", "Private Wealth Management", "Others", "Risques"]
                for d in domaines:
                    if d.lower() in full_text:
                        job_family_raw = d
                        break
            if not country_raw:
                pays = ["France", "Allemagne", "Germany", "Suisse", "Switzerland", "Luxembourg"]
                for p in pays:
                    if p.lower() in full_text:
                        country_raw = p
                        break

            # Mapper famille vers Taleos
            job_family = map_oddo_bhf_family(job_family_raw) if job_family_raw else None
            if not job_family:
                job_family = classify_job_family(job_title, full_text)

            # Location
            location = build_location(city_raw, country_raw)
            if not location and country_raw:
                location = normalize_country(country_raw)

            # Contract normalisé
            contract_type = normalize_contract(contract_type) if contract_type else None

            # Description complète
            desc_parts = []
            for section in soup.select("section, .description, .content, [class*='description']"):
                h = section.select_one("h2, h3, h4")
                if h:
                    title_section = h.get_text(strip=True).lower()
                    if any(kw in title_section for kw in ["beschreibung", "description", "mission", "aufgaben", "anforderung", "profil"]):
                        desc_parts.append(section.get_text(separator="\n", strip=True))
            job_description = " ".join(desc_parts)[:25000] if desc_parts else soup.get_text(separator=" ", strip=True)[:25000]

            # Expérience
            experience_level = extract_experience_level(job_description, contract_type, job_title)

            # Date publication
            if not publication_date:
                publication_date = datetime.now().strftime("%Y-%m-%d")

            job.update({
                "job_id": job_id,
                "job_title": job_title,
                "contract_type": contract_type,
                "publication_date": publication_date,
                "location": location,
                "job_family": job_family,
                "job_description": job_description,
                "experience_level": experience_level,
                "company_name": "ODDO BHF",
                "status": "Live",
            })

        except Exception as e:
            logging.warning(f"Failed to fetch {job.get('job_url')}: {e}")
        finally:
            await page.close()

    return job


# =========================================================
# MAIN
# =========================================================
async def main():
    start = time.time()
    logging.info("=" * 80)
    logging.info("DÉBUT PIPELINE ODDO BHF JOB SCRAPER")
    logging.info("=" * 80)

    db = JobDatabase(config.DB_PATH)
    logging.info(f"Base de données : {config.DB_PATH}")

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=config.HEADLESS)
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
        )

        await context.route(
            "**/*",
            lambda r: r.abort() if r.request.resource_type in config.BLOCK_RESOURCES else r.continue_()
        )

        # Étape 1: Collecter toutes les URLs
        logging.info("\n📋 ÉTAPE 1: Collection des URLs")
        jobs = await collect_all_job_urls(context)
        if not jobs:
            logging.error("Aucune offre trouvée. Vérifiez la structure du site.")
            await browser.close()
            return

        all_current_urls = {j["job_url"] for j in jobs}
        existing_live = db.get_live_urls()
        new_urls = all_current_urls - existing_live
        expired_urls = existing_live - all_current_urls

        logging.info(f"✅ Nouvelles offres : {len(new_urls)}")
        logging.info(f"❌ Offres expirées : {len(expired_urls)}")

        if expired_urls:
            db.mark_as_expired(expired_urls)

        # Étape 2: Scraper les détails (nouveaux uniquement, ou tous si --refresh-all)
        refresh_all = "--refresh-all" in __import__("sys").argv
        if refresh_all:
            jobs_to_detail = jobs
            logging.info(f"\n🔄 Mode --refresh-all: re-scraping des détails pour {len(jobs_to_detail)} offres")
        else:
            jobs_to_detail = [j for j in jobs if j["job_url"] in new_urls]
        if jobs_to_detail:
            logging.info(f"\n🚀 ÉTAPE 2: Scraping de {len(jobs_to_detail)} offres")
            sem = asyncio.Semaphore(config.MAX_CONCURRENT_PAGES)
            tasks = [fetch_job_detail(context, j, sem) for j in jobs_to_detail]
            for coro in tqdm(asyncio.as_completed(tasks), total=len(tasks), desc="Scraping détails"):
                await coro

            today = datetime.now().strftime("%Y-%m-%d")
            for job in jobs_to_detail:
                existing_date = db.get_existing_publication_date(job.get("job_url") or "")
                job["publication_date"] = existing_date or today
                db.insert_or_update_job(job)
        else:
            logging.info("\n✓ Aucune nouvelle offre à scraper")

        await browser.close()

    # Export CSV
    logging.info("\n💾 ÉTAPE 3: Export CSV")
    db.export_to_csv(config.CSV_PATH)

    # Stats
    with sqlite3.connect(config.DB_PATH) as conn:
        row = conn.execute("""
            SELECT
                COUNT(*) as total,
                SUM(CASE WHEN status = 'Live' THEN 1 ELSE 0 END) as live,
                SUM(CASE WHEN status = 'Expired' THEN 1 ELSE 0 END) as expired
            FROM jobs
        """).fetchone()
        logging.info("\n" + "=" * 60)
        logging.info("📊 STATISTIQUES")
        logging.info("=" * 60)
        logging.info(f"Total : {row[0]} | Live : {row[1]} | Expired : {row[2]}")
        logging.info("=" * 60)

    logging.info(f"Durée : {time.time() - start:.1f}s")


if __name__ == "__main__":
    asyncio.run(main())
