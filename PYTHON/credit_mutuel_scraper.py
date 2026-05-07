#!/usr/bin/env python3
"""
CRÉDIT MUTUEL - JOB SCRAPER (v3)
Extrait les offres d'emploi depuis recrutement.creditmutuel.fr

Architecture :
  - requests + BeautifulSoup pour la phase listing (form POST "Plus de résultats")
    → plus de Playwright nécessaire, fonctionne en CI headless sans bot-detection
  - requests + BeautifulSoup pour les détails (parallèle)
  - Delta scraping : seules les nouvelles offres sont scrapées en détail ;
    les offres disparues du listing sont marquées Expired immédiatement.

Mécanique listing :
  Le site utilise la plateforme e-i.com (DevBooster). Le bouton "Plus de résultats"
  soumet un formulaire POST vers l'URL principale avec le champ _FID_PlusDoffres.
  Le serveur retourne une page HTML complète avec 30 offres supplémentaires cumulées.
  On répète jusqu'à atteindre le compte cible ou stagnation.
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

# ================= Config =================
class Config:
    # Listing requests
    MAX_LOAD_MORE_ROUNDS = 200    # max de POSTs "Plus de résultats"
    MAX_STAGNANT_ROUNDS  = 3      # arrêt si N rounds consécutifs sans nouvelles URLs

    # Détails requests
    MAX_WORKERS      = 8          # parallélisme HTTP pour les détails
    REQUEST_TIMEOUT  = 30         # secondes par requête (listing + détails)
    DETAIL_MAX_RETRY = 3

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


def extract_expected_offer_count(page_text: str) -> Optional[int]:
    text = str(page_text or "").replace("\xa0", " ")
    patterns = [
        r'(\d+)\s+offres?\s+affich(?:e|é)es?\s+sur\s+(\d+)',
        r'parmi\s+nos\s+(\d+)\s+offres',
        r'(\d[\d\s]*)\s+offres?\s+correspondent',
    ]
    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if not match:
            continue
        if len(match.groups()) >= 2:
            raw = match.group(2).replace(" ", "").replace("\xa0", "")
        else:
            raw = match.group(1).replace(" ", "").replace("\xa0", "")
        try:
            return int(raw)
        except ValueError:
            continue
    return None


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

    def get_without_details(self, limit: int) -> List[str]:
        """URLs Live sans job_description, par ordre d'arrivée décroissant (les plus récentes en premier)."""
        with sqlite3.connect(self.db_path) as conn:
            rows = conn.execute("""
                SELECT job_url FROM jobs
                WHERE status = 'Live' AND is_valid = 1
                  AND (job_description IS NULL OR TRIM(job_description) = '')
                ORDER BY first_seen DESC
                LIMIT ?
            """, (limit,)).fetchall()
        return [r[0] for r in rows]

    def insert_listing_only(self, job_url: str, job_id: str = "", company_name: str = "Crédit Mutuel"):
        """Insère une offre avec les données minimales du listing (URL + ID).
        Préserve les champs déjà renseignés si l'offre existe déjà."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                INSERT INTO jobs (job_url, job_id, company_name, status, is_valid)
                VALUES (?, ?, ?, 'Live', 1)
                ON CONFLICT(job_url) DO UPDATE SET
                    status       = 'Live',
                    last_updated = CURRENT_TIMESTAMP
            """, (job_url, job_id, company_name))
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
# LISTING — collecte des URLs via form POST (pure requests)
# =========================================================

# URL de base du POST : params fixes reflétant l'état de filtres vides.
# Le serveur e-i.com attend ces paramètres dans la query string du POST.
_POST_QS = (
    "?_tabi=RHEC&_pid=Offres"
    "&k_tc=&k_fp=&k_dpts=&k_locs=&k_pays=0"
    "&k_motsCles=&k_iAnnonces=&k_iSocietes=&k_to=False"
)
_POST_HEADERS = {
    "Origin": BASE_URL,
    "Referer": LISTING_URL,
    "Content-Type": "application/x-www-form-urlencoded",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "fr-FR,fr;q=0.9,en;q=0.8",
}


def _extract_ids_from_soup(soup: BeautifulSoup) -> Set[str]:
    """Extrait tous les IDs d'offres (annonce=XXXXX) depuis le HTML parsé."""
    ids: Set[str] = set()
    for a in soup.find_all("a", href=True):
        m = re.search(r'annonce=(\d+)', a["href"])
        if m:
            ids.add(m.group(1))
    return ids


def _extract_form_data(soup: BeautifulSoup) -> dict:
    """Extrait tous les champs du formulaire pour les soumettre tels quels."""
    data: dict = {}
    form = soup.find("form")
    if not form:
        return data
    for inp in form.find_all("input"):
        name = inp.get("name", "")
        if not name:
            continue
        inp_type = (inp.get("type") or "text").lower()
        if inp_type in ("submit", "button", "image"):
            continue
        if inp_type == "checkbox":
            if inp.get("checked"):
                data[name] = inp.get("value", "on")
            # cases non cochées : ne pas inclure (comportement navigateur)
        elif inp_type == "radio":
            if inp.get("checked"):
                data[name] = inp.get("value", "")
        else:
            data[name] = inp.get("value", "")
    for sel in form.find_all("select"):
        name = sel.get("name", "")
        if not name:
            continue
        opt = sel.find("option", selected=True)
        if opt is None:
            opt = sel.find("option")  # première option par défaut
        data[name] = opt.get("value", "") if opt else ""
    return data


def collect_all_job_ids(session: requests.Session) -> Set[str]:
    """Collecte tous les IDs d'offres via form POST répété (sans Playwright).

    Stratégie :
      1. GET de la page listing → extraction $CPT + IDs initiaux.
      2. Boucle : POST avec _FID_PlusDoffres → réponse HTML cumulative
         → extraction des nouveaux IDs → mise à jour $CPT → répétition.
      3. Arrêt quand objectif atteint ou MAX_STAGNANT_ROUNDS sans progression.
    """
    all_ids: Set[str] = set()

    # ── Étape 1 : chargement initial ─────────────────────────────────────────
    logging.info(f"  Chargement listing CM: {LISTING_URL}")
    try:
        resp = session.get(LISTING_URL, timeout=config.REQUEST_TIMEOUT)
        resp.raise_for_status()
    except Exception as e:
        logging.error(f"  Erreur GET listing CM: {e}")
        return all_ids

    soup = BeautifulSoup(resp.text, "html.parser")

    # Nombre cible d'offres (champ caché Data_NbOffresChargees)
    target_count: Optional[int] = None
    nb_el = soup.find("input", {"name": "Data_NbOffresChargees"})
    if nb_el:
        try:
            target_count = int(nb_el.get("value", 0) or 0)
        except (ValueError, TypeError):
            pass
    if not target_count:
        body_text = soup.get_text(" ", strip=True)
        target_count = extract_expected_offer_count(body_text)
    if target_count:
        logging.info(f"  Cible annoncée: {target_count} offres")
    else:
        logging.warning("  Nombre d'offres attendu introuvable")

    # IDs de la page initiale
    ids = _extract_ids_from_soup(soup)
    all_ids.update(ids)
    logging.info(f"  Page initiale: {len(all_ids)} IDs")

    # ── Étape 2 : boucle POST ─────────────────────────────────────────────────
    post_url = LISTING_URL + _POST_QS
    stagnant_rounds = 0

    for round_idx in range(config.MAX_LOAD_MORE_ROUNDS):
        # Objectif atteint
        if target_count and len(all_ids) >= target_count:
            logging.info(f"  ✅ Objectif atteint ({len(all_ids)}/{target_count})")
            break

        # Extraire les champs du formulaire depuis la dernière réponse
        form_data = _extract_form_data(soup)
        if not form_data.get("$CPT"):
            logging.warning("  ⚠️  $CPT manquant dans le formulaire — arrêt")
            break

        # Ajouter l'action "Plus de résultats"
        form_data["_FID_PlusDoffres"] = ""

        try:
            resp = session.post(
                post_url,
                data=form_data,
                headers=_POST_HEADERS,
                timeout=config.REQUEST_TIMEOUT,
            )
            resp.raise_for_status()
        except Exception as e:
            logging.error(f"  Erreur POST tour {round_idx + 1}: {e}")
            break

        soup = BeautifulSoup(resp.text, "html.parser")
        new_ids = _extract_ids_from_soup(soup)

        prev_count = len(all_ids)
        all_ids.update(new_ids)
        curr_count = len(all_ids)

        logging.info(
            f"  Tour {round_idx + 1:3d}: {curr_count} IDs"
            + (f" / {target_count}" if target_count else "")
        )

        if curr_count == prev_count:
            stagnant_rounds += 1
            logging.warning(f"  Pas de progression ({stagnant_rounds}/{config.MAX_STAGNANT_ROUNDS})")
            if stagnant_rounds >= config.MAX_STAGNANT_ROUNDS:
                logging.warning(
                    f"  ⚠️  Arrêt après {config.MAX_STAGNANT_ROUNDS} tours sans progression"
                    + (f" ({curr_count}/{target_count})" if target_count else f" ({curr_count})")
                )
                break
        else:
            stagnant_rounds = 0

        time.sleep(0.5)

    logging.info(f"  Total IDs collectés: {len(all_ids)}")
    return all_ids


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
            "Mozilla/5.0 (X11; Linux x86_64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
        ),
        "Accept-Language": "fr-FR,fr;q=0.9,en;q=0.8",
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
        for future in tqdm(as_completed(futures), total=len(futures), desc="Détails CM", ncols=80):
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
    logging.info("CRÉDIT MUTUEL - DÉBUT DU SCRAPING")
    logging.info("=" * 60)

    # ── 0. Nettoyage : pages d'erreur précédemment scrapées ───────────────
    n_invalid = db.mark_error_pages_invalid()
    if n_invalid:
        logging.info(f"🧹 {n_invalid} offres (pages d'erreur) marquées invalides")

    # ── 1. Collecte des IDs depuis le listing ──────────────────────────────
    logging.info("\n📋 ÉTAPE 1: Collecte des IDs via le listing CM")
    all_ids = collect_all_job_ids(session)
    if not all_ids:
        logging.error("❌ Aucun ID collecté — arrêt")
        return

    # Construire les URLs à partir des IDs
    all_current_urls = {
        f"{BASE_URL}/fr/offre.html?annonce={aid}" for aid in all_ids
    }
    logging.info(f"  Total URLs: {len(all_current_urls)}")

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

    # ── 4. Insérer IMMÉDIATEMENT les nouvelles offres (listing-only) ───────
    if new_urls:
        logging.info(f"\n💾 ÉTAPE 4: Insertion listing-only de {len(new_urls)} nouvelles offres")
        for url in new_urls:
            m = re.search(r'annonce=(\d+)', url)
            job_id = f"CM_{m.group(1)}" if m else ""
            db.insert_listing_only(url, job_id)
        logging.info(f"  ✓ {len(new_urls)} offres insérées (visibles dans l'export)")

    # ── 5. Scraping des détails (toutes les offres sans description) ───────
    # CM a ~900 offres max, requests parallèle est rapide.
    # ~900 offres à 8 workers = < 5 min en général.
    backlog_count = db.count_without_details()

    if backlog_count == 0:
        logging.info("\n✓ Toutes les offres ont déjà leurs détails")
    else:
        urls_to_detail = db.get_without_details(limit=backlog_count)
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
