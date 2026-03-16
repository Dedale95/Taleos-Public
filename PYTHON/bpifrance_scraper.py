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

# ================= BPI criteres (famille de métier) → nom affiché =================
# Mapping des slugs ?criteres=XXX vers les familles Taleos (harmonisé job_family_classifier)
BPI_CRITERES_TO_JOB_FAMILY = {
    "reseau-developpement-commercial-et-relation-client": "Commercial / Relations Clients",
    "reseau-bancaire": "Commercial / Relations Clients",
    "investissement-capital-developpement": "Financement et Investissement",
    "capital-investissement": "Financement et Investissement",
    "investissement": "Financement et Investissement",
    "conformite-controle-permanent": "Conformité / Sécurité financière",
    "conduite-conformite": "Conformité / Sécurité financière",
    "conseil": "Conseil",
    "developpement-durable-rse": "Développement durable et RSE",
    "digital-it": "IT, Digital et Data",
    "finances": "Finances / Comptabilité / Contrôle de gestion",
    "innovation": "Innovation",
    "inspection-generale-audit": "Risques / Contrôles permanents",
    "inspection-audit": "Risques / Contrôles permanents",
    "international": "International",
    "juridique": "Juridique",
    "marketing-communication-relations-publiques": "Marketing et Communication",
    "ressources-humaines": "Ressources Humaines",
    "risques": "Risques / Contrôles permanents",
    "strategie-etudes": "Stratégie et études",
}


def criteres_to_job_family(criteres_slug: Optional[str]) -> Optional[str]:
    """Convertit le slug criteres BPI en famille de métier Taleos."""
    if not criteres_slug:
        return None
    slug = criteres_slug.strip().lower()
    return BPI_CRITERES_TO_JOB_FAMILY.get(slug)


def normalize_contract(raw: Optional[str]) -> Optional[str]:
    if not raw:
        return None
    cleaned = str(raw).strip().lower()
    for key, value in CONTRACT_MAPPING.items():
        if key in cleaned:
            return value
    return raw.strip()


def extract_location_from_title_and_description(title: Optional[str], description: Optional[str]) -> Optional[str]:
    """
    Extrait la localisation depuis le titre ou la description quand les champs standards ne la fournissent pas.
    Ex: "Chargé d'études Réunion" → La Réunion, "Direction interrégionale Outre-mer" + contexte Réunion
    """
    text = " ".join(filter(None, [title or "", description or ""])).lower()
    if not text:
        return None
    # DOM-TOM : titre "X Réunion" ou description "Outre-mer" + "Réunion"
    dom_tom_patterns = [
        (r'\bréunion\b', 'La Réunion'),
        (r'\bmartinique\b', 'Martinique'),
        (r'\bguadeloupe\b', 'Guadeloupe'),
        (r'\bguyane\b', 'Guyane'),
        (r'polynésie\s+française|polynesie\s+francaise', 'Polynésie Française'),
        (r'nouvelle-calédonie|nouvelle-caledonie', 'Nouvelle-Calédonie'),
    ]
    for pattern, loc_name in dom_tom_patterns:
        if re.search(pattern, text, re.IGNORECASE):
            city = normalize_city(loc_name)
            if city:
                country = get_country_from_city(city) or "France"
                country = normalize_country(country)
                return f"{city} - {country}"
    return None


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

                # Contrat et lieu: sur la vignette "CDI • Paris (haussmann)" ou "Alternance • Maisons-Alfort"
                # Le texte peut être dans le lien ou dans un parent/sibling
                link_text = link.get_text(strip=True).replace("Voir le détail", "").strip()
                contract_type = None
                location_raw = None
                if " • " in link_text:
                    parts = link_text.split(" • ", 1)
                    contract_type = normalize_contract(parts[0].strip()) if parts else None
                    location_raw = parts[1].strip() if len(parts) > 1 else None
                # Fallback: chercher dans le bloc parent (ex: div contenant h3 + meta + lien)
                if not contract_type or not location_raw:
                    block = link.find_parent(["div", "article", "li"]) or link
                    block_text = block.get_text(separator=" ", strip=True) if block else ""
                    block_text = block_text.replace("Voir le détail", "").strip()
                    if " • " in block_text:
                        parts = block_text.split(" • ", 2)
                        if len(parts) >= 2:
                            if not contract_type:
                                contract_type = normalize_contract(parts[0].strip())
                            if not location_raw:
                                location_raw = parts[1].replace("Voir le détail", "").strip()

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
    """Enrichit un job avec la description et les tags explicites depuis la page détail."""
    async with sem:
        page = await context.new_page()
        try:
            await page.goto(job["job_url"], timeout=config.PAGE_TIMEOUT, wait_until="domcontentloaded")
            await asyncio.sleep(1)
            html = await page.content()
            soup = BeautifulSoup(html, "html.parser")

            # Titre
            h1 = soup.select_one("h1")
            if h1:
                job["job_title"] = h1.get_text(strip=True)

            # === TYPE DE CONTRAT ===
            # Priorité 1: garder celui de la liste (vignette) - source fiable
            # Priorité 2: header "ALTERNANCE • TEMPS-PLEIN • MAISONS-ALFORT"
            # Priorité 3: LIs de "Détails de l'offre" uniquement (jamais "Ces offres")
            if not job.get("contract_type"):
                # Header meta: "ALTERNANCE • TEMPS-PLEIN • MAISONS-ALFORT" (1er = contrat, 2e = durée, 3e = lieu)
                header = soup.select_one(".entry-header, .job-header, [class*='header']")
                if header:
                    meta_text = header.get_text(strip=True)
                    if " • " in meta_text:
                        first_part = meta_text.split(" • ")[0].strip().lower()
                        # Ignorer "temps-plein", "temps plein" (durée, pas type de contrat)
                        if first_part and first_part not in ("temps-plein", "temps plein"):
                            ct = normalize_contract(first_part)
                            if ct and ct in list(CONTRACT_MAPPING.values()) + ["Alternance / Apprentissage"]:
                                job["contract_type"] = ct
                # Fallback: LIs de "Détails de l'offre" (exclure "Ces offres")
                if not job.get("contract_type"):
                    ces_offres_h2 = soup.find("h2", string=re.compile(r"Ces offres", re.I))
                    for li in soup.find_all("li"):
                        if ces_offres_h2 and li.find_previous("h2", string=re.compile(r"Ces offres", re.I)):
                            continue
                        txt = li.get_text(strip=True)
                        if not txt or len(txt) > 50:
                            continue
                        txt_lower = txt.lower()
                        if txt_lower in ("cdi", "cdd", "stage", "vie", "v.i.e"):
                            job["contract_type"] = normalize_contract(txt)
                            break
                        if txt_lower == "alternance":
                            job["contract_type"] = "Alternance / Apprentissage"
                            break

            # Localisation: "Vos futurs bureaux : Paris (Haussmann)" ou header "cdi • Paris (Haussmann)"
            for h3 in soup.select("h3"):
                bureau_text = h3.get_text(strip=True)
                if "bureau" in bureau_text.lower() and ":" in bureau_text:
                    loc_match = re.search(r":\s*(.+)", bureau_text)
                    if loc_match:
                        job["location"] = build_location(loc_match.group(1).strip())
                        break
            if not job.get("location"):
                header_meta = soup.select_one(".entry-header .meta, .job-meta, [class*='meta']")
                if header_meta:
                    meta_text = header_meta.get_text(strip=True)
                    if " • " in meta_text:
                        parts = meta_text.split(" • ")
                        if len(parts) > 1:
                            loc_raw = " ".join(parts[1:])  # après le contrat
                            job["location"] = build_location(loc_raw)

            # Fallback header si pas encore rempli
            if not job.get("contract_type") or not job.get("location"):
                header_meta = soup.select_one(".entry-header .meta, .job-meta, [class*='meta']")
                if header_meta:
                    meta_text = header_meta.get_text(strip=True).lower()
                    if " • " in meta_text:
                        parts = meta_text.split(" • ")
                        if not job.get("contract_type") and parts:
                            job["contract_type"] = normalize_contract(parts[0])
                        if not job.get("location") and len(parts) > 1:
                            job["location"] = build_location(" ".join(parts[1:]))

            # === FAMILLE DE MÉTIER: lien "Voir plus d'offres" → ?criteres=XXX ===
            voir_plus = soup.select_one('a[href*="criteres="]')
            if voir_plus:
                href = voir_plus.get("href", "")
                criteres_m = re.search(r"criteres=([a-z0-9\-]+)", href, re.I)
                if criteres_m:
                    slug = criteres_m.group(1).strip().lower()
                    job_family_from_tag = criteres_to_job_family(slug)
                    if job_family_from_tag:
                        job["job_family"] = job_family_from_tag

            # Description: contenu principal
            main = soup.select_one("article .entry-content, .job-description, .offer-content, [class*='content']")
            if main:
                for skip in main.select(".related-jobs, .similar-offers, [class*='related']"):
                    skip.decompose()
                desc_text = main.get_text(separator="\n", strip=True)
                desc_text = unescape(desc_text)
                desc_text = re.sub(r'\s+', ' ', desc_text).strip()[:25000]
                job["job_description"] = desc_text
            else:
                job["job_description"] = ""

            # Fallback job_family par classifier si pas de tag criteres
            desc = job.get("job_description", "")
            if not job.get("job_family") and desc:
                job["job_family"] = classify_job_family(job.get("job_title", ""), desc)
            job["education_level"] = extract_education_level(desc)
            job["experience_level"] = extract_experience_level(desc, job.get("contract_type"), job.get("job_title"))

            # Fallback: extraire localisation depuis titre/description (ex: "Chargé d'études Réunion", "Direction Outre-mer")
            if not job.get("location"):
                loc = extract_location_from_title_and_description(job.get("job_title"), desc)
                if loc:
                    job["location"] = loc

        except Exception as e:
            logging.warning(f"Erreur détail {job.get('job_url')}: {e}")
        finally:
            await page.close()


# =========================================================
# MAIN
# =========================================================
async def main(refresh_all: bool = False):
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

        # Étape 4: Scraper les détails (nouveaux uniquement, ou tous si --refresh-all)
        if refresh_all:
            jobs_to_detail = all_jobs
            logging.info(f"\n🔄 Mode --refresh-all: re-scraping des détails pour {len(jobs_to_detail)} offres")
        else:
            jobs_to_detail = [j for j in all_jobs if j.get("job_url") in new_urls]
        if jobs_to_detail:
            logging.info(f"\n🚀 ÉTAPE 4: Scraping des détails de {len(jobs_to_detail)} offres")
            tasks = [fetch_job_detail(context, job, sem) for job in jobs_to_detail]
            for coro in tqdm(asyncio.as_completed(tasks), total=len(tasks), desc="Détails"):
                await coro
            today = datetime.now().strftime("%Y-%m-%d")
            for job in jobs_to_detail:
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
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--refresh-all", action="store_true", help="Re-scraper les détails de toutes les offres (corrige les types de contrat erronés)")
    args = parser.parse_args()
    asyncio.run(main(refresh_all=args.refresh_all))
