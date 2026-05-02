#!/usr/bin/env python3
"""
CRÉDIT MUTUEL - JOB SCRAPER (v3)
Extrait les offres d'emploi depuis recrutement.creditmutuel.fr

Architecture :
  - Listing : une seule requête POST avec Data_NbPages=100 → tous les résultats
    en une fois (pas de Playwright, pas de "clic" itératif)
  - Détails : requests + BeautifulSoup en parallèle (ThreadPoolExecutor)
  - Delta scraping : les nouvelles URLs sont insérées immédiatement avec les
    données du listing (titre, contrat, localisation, date) — visibles dans
    l'export sans attendre le scraping détail. Les offres disparues sont
    marquées Expired.
"""

import logging
import re
import sqlite3
import json
import time
from pathlib import Path
from typing import List, Dict, Set, Optional
from bs4 import BeautifulSoup
from tqdm import tqdm
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
    from credit_mutuel_company_mapping import normalize_company_name
except ImportError:
    import sys
    sys.path.append(str(Path(__file__).parent))
    from city_normalizer import normalize_city
    from country_normalizer import normalize_country, get_country_from_city
    from job_family_classifier import classify_job_family
    from experience_extractor import extract_experience_level
    from credit_mutuel_job_family_mapping import map_credit_mutuel_family
    from credit_mutuel_company_mapping import normalize_company_name

# ================= Logging =================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# ================= Constants =================
BASE_URL = "https://recrutement.creditmutuel.fr"
LISTING_URL = f"{BASE_URL}/fr/nos_offres.html"

# Headers Firefox — évite les blocages Akamai / Cloudflare
LISTING_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:125.0) Gecko/20100101 Firefox/125.0",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7",
    "Accept-Encoding": "gzip, deflate, br",
    "Content-Type": "application/x-www-form-urlencoded",
    "Origin": BASE_URL,
    "Referer": LISTING_URL,
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
}

# POST body : Data_NbPages=100 → le serveur renvoie jusqu'à 1 500 offres en une page
LISTING_BODY = {
    "Data_PaysSelec": "127",   # France
    "Data_NbPages": "100",     # 100 pages × ~15 offres/page = toutes les offres
    "_FID_PlusDoffres": "",    # déclenche le "load-more" côté serveur
}

# ================= Config =================
class Config:
    # Détails requests
    MAX_WORKERS      = 8    # parallélisme HTTP pour les détails
    REQUEST_TIMEOUT  = 20   # secondes par requête
    DETAIL_MAX_RETRY = 3

    LISTING_TIMEOUT  = 60   # secondes pour la requête listing POST

    BASE_DIR = Path(__file__).parent
    DB_PATH  = BASE_DIR / "credit_mutuel_jobs.db"
    CSV_PATH = BASE_DIR / "credit_mutuel_jobs.csv"

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
    "auxiliaire de vacances": "Stage",
    "reconversion professionnelle (cdi)": "CDI",
}

EXPERIENCE_MAPPING = {
    "débutant":  "0 - 2 ans",
    "debutant":  "0 - 2 ans",
    "junior":    "0 - 2 ans",
    "confirmé":  "6 - 10 ans",
    "confirme":  "6 - 10 ans",
    "senior":    "11 ans et plus",
    "expert":    "11 ans et plus",
}

ERROR_PATTERNS = [
    'erreur de navigation', 'accusé de réception', 'accuse de reception',
    'page not found', 'page introuvable',
]


def normalize_contract(raw: str) -> str:
    if not raw:
        return ""
    cleaned = re.sub(r'\s*\([^)]*\)\s*', '', raw).strip().lower()
    return CONTRACT_MAPPING.get(cleaned, raw.strip())


def normalize_experience(raw: str) -> Optional[str]:
    if not raw:
        return None
    return EXPERIENCE_MAPPING.get(str(raw).strip().lower())


def normalize_location_cm(location_raw: str) -> str:
    """CM format: 'VILLE (XX)' → 'Ville - France' / 'Luxembourg - Luxembourg'."""
    if not location_raw or not str(location_raw).strip():
        return ""
    loc = str(location_raw).strip()
    match = re.match(r'^(.+?)\s*\(([^)]+)\)\s*$', loc)
    if match:
        city_raw = match.group(1).strip()
        code = match.group(2).strip()
        if code.upper() == "LUXEMBOURG":
            return f"{normalize_city(city_raw) or city_raw} - Luxembourg"
        if re.match(r'^(0[1-9]|[1-8]\d|9[0-5]|2[AB]|97[1-6])$', code, re.I):
            city = normalize_city(city_raw) or city_raw
            return f"{city} - France"
    city = normalize_city(loc) or loc
    return f"{city} - France"


def parse_date_cm(raw: str) -> str:
    """Parse 'Date de publication : 01/05/2025' → '2025-05-01'."""
    if not raw:
        return ""
    cleaned = re.sub(r'^.*?:', '', raw).strip()
    m = re.search(r'(\d{2})/(\d{2})/(\d{4})', cleaned)
    if m:
        return f"{m.group(3)}-{m.group(2)}-{m.group(1)}"
    return cleaned


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

    def count_without_details(self) -> int:
        with sqlite3.connect(self.db_path) as conn:
            row = conn.execute("""
                SELECT COUNT(*) FROM jobs
                WHERE status = 'Live' AND is_valid = 1
                  AND (job_description IS NULL OR TRIM(job_description) = '')
            """).fetchone()
        return row[0] if row else 0

    def get_without_details(self, limit: int = 9999) -> List[str]:
        """URLs Live sans job_description — ordre aléatoire pour éviter les doublons entre runs."""
        with sqlite3.connect(self.db_path) as conn:
            rows = conn.execute("""
                SELECT job_url FROM jobs
                WHERE status = 'Live' AND is_valid = 1
                  AND (job_description IS NULL OR TRIM(job_description) = '')
                ORDER BY scrape_attempts ASC, RANDOM()
                LIMIT ?
            """, (limit,)).fetchall()
        return [r[0] for r in rows]

    def insert_listing_only(
        self,
        job_url: str,
        job_id: str = "",
        company_name: str = "Crédit Mutuel",
        job_title: str = "",
        contract_type: str = "",
        location: str = "",
        publication_date: str = "",
    ):
        """Insère une offre avec les données du listing.
        Préserve les données détail déjà renseignées si l'offre existe.
        """
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                INSERT INTO jobs (
                    job_url, job_id, company_name, job_title,
                    contract_type, location, publication_date,
                    status, is_valid
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, 'Live', 1)
                ON CONFLICT(job_url) DO UPDATE SET
                    status           = 'Live',
                    last_updated     = CURRENT_TIMESTAMP,
                    job_title        = CASE WHEN COALESCE(jobs.job_title, '') = ''
                                           THEN excluded.job_title ELSE jobs.job_title END,
                    contract_type    = CASE WHEN COALESCE(jobs.contract_type, '') = ''
                                           THEN excluded.contract_type ELSE jobs.contract_type END,
                    location         = CASE WHEN COALESCE(jobs.location, '') = ''
                                           THEN excluded.location ELSE jobs.location END,
                    publication_date = CASE WHEN COALESCE(jobs.publication_date, '') = ''
                                           THEN excluded.publication_date ELSE jobs.publication_date END
            """, (job_url, job_id, company_name, job_title,
                  contract_type, location, publication_date))
            conn.commit()

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

    def mark_error_pages_invalid(self) -> int:
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                "SELECT job_url, job_title, job_description FROM jobs "
                "WHERE is_valid = 1 AND (job_title IS NOT NULL OR job_description IS NOT NULL)"
            )
            to_invalidate = []
            for row in cursor.fetchall():
                title = (row[1] or '').lower().replace('\xa0', ' ')
                desc  = (row[2] or '').lower().replace('\xa0', ' ')
                if any(p in title for p in ERROR_PATTERNS) or any(p in desc for p in ERROR_PATTERNS):
                    to_invalidate.append(row[0])
            if to_invalidate:
                ph = ','.join('?' * len(to_invalidate))
                conn.execute(
                    f"UPDATE jobs SET is_valid = 0, last_updated = CURRENT_TIMESTAMP "
                    f"WHERE job_url IN ({ph})",
                    tuple(to_invalidate)
                )
                conn.commit()
        return len(to_invalidate)

    def insert_or_update_job(self, job: Dict):
        is_valid = 1 if (job.get('job_id') or job.get('job_title') or job.get('job_description')) else 0
        title_lower = (job.get('job_title') or '').lower().replace('\xa0', ' ')
        desc_lower  = (job.get('job_description') or '').lower().replace('\xa0', ' ')
        if any(e in title_lower for e in ERROR_PATTERNS) or any(e in desc_lower for e in ERROR_PATTERNS):
            is_valid = 0

        technical_skills  = job.get('technical_skills') or '[]'
        behavioral_skills = job.get('behavioral_skills') or '[]'
        if isinstance(technical_skills, list):
            technical_skills  = json.dumps(technical_skills, ensure_ascii=False)
        if isinstance(behavioral_skills, list):
            behavioral_skills = json.dumps(behavioral_skills, ensure_ascii=False)

        with sqlite3.connect(self.db_path) as conn:
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
                    job_id            = excluded.job_id,
                    job_title         = excluded.job_title,
                    contract_type     = excluded.contract_type,
                    publication_date  = excluded.publication_date,
                    location          = excluded.location,
                    job_family        = excluded.job_family,
                    status            = excluded.status,
                    education_level   = excluded.education_level,
                    experience_level  = excluded.experience_level,
                    job_description   = excluded.job_description,
                    company_name      = excluded.company_name,
                    company_description = excluded.company_description,
                    scrape_attempts   = scrape_attempts + 1,
                    is_valid          = excluded.is_valid,
                    last_updated      = CURRENT_TIMESTAMP
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
# LISTING — collecte via POST (pas de Playwright)
# =========================================================
def collect_all_jobs_from_listing(session: requests.Session) -> Dict[str, Dict]:
    """
    Récupère toutes les offres du listing en une seule requête POST.

    Le paramètre Data_NbPages=100 demande au serveur (e-i.com ATS) de renvoyer
    jusqu'à 100 pages de résultats en une seule réponse HTML — soit jusqu'à ~1 500
    offres. Aucun Playwright ni clic itératif nécessaire.

    Retourne un dict {job_url: {job_id, job_title, contract_type, location, publication_date}}.
    """
    logging.info(f"  POST listing: {LISTING_URL} (Data_NbPages=100)")
    try:
        resp = session.post(
            LISTING_URL,
            headers=LISTING_HEADERS,
            data=LISTING_BODY,
            timeout=config.LISTING_TIMEOUT,
        )
        resp.raise_for_status()
    except Exception as e:
        logging.error(f"❌ Erreur requête listing: {e}")
        return {}

    soup = BeautifulSoup(resp.text, "html.parser")

    # Compter le nombre annoncé par le site
    page_text = soup.get_text(" ", strip=True)
    m_count = re.search(r'(\d[\d\s]*)\s+offres?\s+correspondent', page_text, re.IGNORECASE)
    if m_count:
        announced = int(m_count.group(1).replace(" ", "").replace("\xa0", ""))
        logging.info(f"  Site annonce : {announced} offres")
    else:
        logging.warning("  Nombre d'offres annoncé non trouvé dans la page")

    results: Dict[str, Dict] = {}

    for card in soup.select('li.item'):
        link = card.select_one('a[href*="annonce="]')
        if not link:
            continue
        href = link.get('href', '')
        m = re.search(r'annonce=(\d+)', href)
        if not m:
            continue

        job_id_raw = m.group(1)
        job_id = f"CM_{job_id_raw}"

        # URL canonique
        if href.startswith('http'):
            job_url = href
        else:
            job_url = BASE_URL + href if href.startswith('/') else f"{BASE_URL}/{href}"

        title = link.get_text(strip=True)

        # Métadonnées dans ul.ei_listdescription
        items = card.select('ul.ei_listdescription li')
        location_raw  = items[0].get_text(strip=True) if len(items) > 0 else ""
        contract_raw  = items[1].get_text(strip=True) if len(items) > 1 else ""
        date_raw      = items[2].get_text(strip=True) if len(items) > 2 else ""

        results[job_url] = {
            "job_id":           job_id,
            "job_title":        title,
            "contract_type":    normalize_contract(contract_raw),
            "location":         normalize_location_cm(location_raw),
            "publication_date": parse_date_cm(date_raw),
        }

    logging.info(f"  {len(results)} offres extraites du listing")
    return results


# =========================================================
# DÉTAILS — scraping (requests + BeautifulSoup)
# =========================================================
def create_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(total=3, backoff_factor=0.5, status_forcelist=[429, 500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.headers.update({
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:125.0) "
            "Gecko/20100101 Firefox/125.0"
        )
    })
    return session


def scrape_job_detail(url: str, session: requests.Session) -> Optional[Dict]:
    """Scrape le détail d'une offre via requests + BeautifulSoup."""
    try:
        r = session.get(url, timeout=config.REQUEST_TIMEOUT)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")

        job: Dict = {
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

        # Filiale (CIC, Cofidis, Euro Information…)
        company_el = soup.select_one(".rhec_detailoffre .ei_subtitle")
        if company_el:
            raw_company = company_el.get_text(strip=True)
            if raw_company and "retour" not in raw_company.lower() and len(raw_company) > 3:
                job["company_name"] = normalize_company_name(raw_company)

        # job_id depuis URL
        m = re.search(r'annonce=(\d+)', url)
        if m:
            job["job_id"] = f"CM_{m.group(1)}"

        # Titre depuis h1
        h1 = soup.select_one("h1")
        if h1:
            job["job_title"] = h1.get_text(strip=True)

        # Tableau détail : label → value (3 colonnes)
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
                    dm = re.search(r'(\d{2})/(\d{2})/(\d{4})', value)
                    if dm:
                        job["publication_date"] = f"{dm.group(3)}-{dm.group(2)}-{dm.group(1)}"
                    else:
                        job["publication_date"] = value

        # Description
        main_content = soup.select_one("main, .content, article, [role='main']") or soup
        desc_parts = []
        for p_el in main_content.find_all(["p", "div"], recursive=True):
            text = p_el.get_text(strip=True)
            if text and len(text) > 50 and "diversité" not in text.lower()[:100]:
                if any(kw in text.lower() for kw in ("mission", "vous ", "nous ", "votre ", "notre ")):
                    desc_parts.append(text)
        if desc_parts:
            job["job_description"] = "\n\n".join(desc_parts[:15])
        if not job["job_description"]:
            job["job_description"] = main_content.get_text(separator="\n", strip=True)[:15000]

        # Fallback job_family
        if not job["job_family"]:
            job["job_family"] = classify_job_family(job.get("job_title", ""), job.get("job_description", ""))

        # Fallback experience_level
        if not job["experience_level"]:
            job["experience_level"] = extract_experience_level(
                job.get("job_description", ""),
                job.get("contract_type"),
                job.get("job_title"),
            )

        return job

    except Exception as e:
        logging.warning(f"Erreur scraping {url}: {e}")
        return None


def scrape_detail_with_retries(url: str, session: requests.Session) -> Optional[Dict]:
    for attempt in range(1, config.DETAIL_MAX_RETRY + 1):
        job = scrape_job_detail(url, session)
        if job:
            return job
        if attempt < config.DETAIL_MAX_RETRY:
            time.sleep(1)
    logging.warning(f"Échec définitif Crédit Mutuel: {url}")
    return None


def scrape_details_parallel(urls: List[str], session: requests.Session) -> List[Dict]:
    """Scrape les détails en parallèle avec ThreadPoolExecutor."""
    results: List[Dict] = []
    failed: List[str] = []

    with ThreadPoolExecutor(max_workers=config.MAX_WORKERS) as executor:
        futures = {executor.submit(scrape_detail_with_retries, u, session): u for u in urls}
        for future in tqdm(as_completed(futures), total=len(futures), desc="Détails CM"):
            job = future.result()
            if job:
                results.append(job)
            else:
                failed.append(futures[future])

    if failed:
        logging.warning(f"  {len(failed)} URLs en échec — nouvelle tentative séquentielle")
        for url in failed:
            job = scrape_job_detail(url, session)
            if job:
                results.append(job)

    return results


# =========================================================
# MAIN PIPELINE
# =========================================================
def main():
    start = time.time()
    db = JobDatabase(config.DB_PATH)
    session = create_session()

    logging.info("=" * 60)
    logging.info("CRÉDIT MUTUEL - DÉBUT DU SCRAPING (v3 — POST listing)")
    logging.info("=" * 60)

    # ── 0. Nettoyage : pages d'erreur précédemment scrapées ───────────────
    n_invalid = db.mark_error_pages_invalid()
    if n_invalid:
        logging.info(f"🧹 {n_invalid} offres (pages d'erreur) marquées invalides")

    # ── 1. Collecte du listing via POST ───────────────────────────────────
    logging.info("\n📋 ÉTAPE 1: Collecte du listing via POST unique")
    listing = collect_all_jobs_from_listing(session)

    if not listing:
        logging.error("❌ Aucune offre collectée depuis le listing — arrêt")
        return

    all_current_urls = set(listing.keys())
    logging.info(f"  Total URLs listing: {len(all_current_urls)}")

    # ── 2. Analyse delta : nouvelles / expirées ────────────────────────────
    logging.info("\n🔍 ÉTAPE 2: Analyse delta")
    existing_live = db.get_live_urls()
    new_urls      = all_current_urls - existing_live
    expired_urls  = existing_live - all_current_urls
    unchanged     = existing_live & all_current_urls

    logging.info(f"  ✅ Nouvelles:  {len(new_urls)}")
    logging.info(f"  ❌ Expirées:   {len(expired_urls)}")
    logging.info(f"  🔄 Inchangées: {len(unchanged)}")

    # ── 3. Expirer les offres disparues du listing ─────────────────────────
    if expired_urls:
        logging.info(f"\n⏳ ÉTAPE 3: Marquage de {len(expired_urls)} offres expirées")
        db.mark_as_expired(expired_urls)

    # ── 4. Insérer les nouvelles offres avec données listing ───────────────
    if new_urls:
        logging.info(f"\n💾 ÉTAPE 4: Insertion listing-only de {len(new_urls)} nouvelles offres")
        for url in new_urls:
            data = listing[url]
            db.insert_listing_only(
                job_url=url,
                job_id=data["job_id"],
                company_name="Crédit Mutuel",
                job_title=data["job_title"],
                contract_type=data["contract_type"],
                location=data["location"],
                publication_date=data["publication_date"],
            )
        logging.info(f"  ✓ {len(new_urls)} offres insérées avec titre/contrat/lieu/date")
    else:
        # Même sans nouvelles offres : remettre à Live au cas où
        logging.info("\n  (pas de nouvelle offre — mise à jour statut Live des offres existantes)")
        for url in all_current_urls:
            data = listing[url]
            db.insert_listing_only(
                job_url=url,
                job_id=data["job_id"],
                company_name="Crédit Mutuel",
                job_title=data["job_title"],
                contract_type=data["contract_type"],
                location=data["location"],
                publication_date=data["publication_date"],
            )

    # ── 5. Scraping des détails (offres sans description) ─────────────────
    backlog_count = db.count_without_details()

    if backlog_count == 0:
        logging.info("\n✓ Toutes les offres ont déjà leurs détails")
    else:
        urls_to_detail = db.get_without_details()
        logging.info(f"\n🚀 ÉTAPE 5: Scraping détails de {len(urls_to_detail)} offres")

        jobs_scraped = scrape_details_parallel(urls_to_detail, session)

        saved = 0
        for job in jobs_scraped:
            db.insert_or_update_job(job)
            saved += 1

        remaining = db.count_without_details()
        logging.info(f"  ✓ {saved} offres complétées")
        if remaining:
            logging.info(f"  📋 {remaining} offres toujours sans détails (next run)")

    # ── Statistiques finales ───────────────────────────────────────────────
    with sqlite3.connect(config.DB_PATH) as conn:
        stats = conn.execute("""
            SELECT
                COUNT(*) as total,
                SUM(CASE WHEN status = 'Live'    AND is_valid = 1 THEN 1 ELSE 0 END) as live,
                SUM(CASE WHEN status = 'Expired'                  THEN 1 ELSE 0 END) as expired,
                SUM(CASE WHEN is_valid = 0                        THEN 1 ELSE 0 END) as invalid
            FROM jobs
        """).fetchone()

    elapsed = time.time() - start
    logging.info("\n" + "=" * 60)
    logging.info("📊 STATISTIQUES FINALES")
    logging.info("=" * 60)
    logging.info(f"  Live (actives) : {stats[1]}")
    logging.info(f"  Expirées       : {stats[2]}")
    logging.info(f"  Invalides      : {stats[3]}")
    logging.info(f"  Durée          : {elapsed:.1f}s")
    logging.info("=" * 60)


if __name__ == "__main__":
    main()
