#!/usr/bin/env python3
"""
BPCE - JOB SCRAPER
Extrait les offres d'emploi du site recrutement BPCE via l'API REST.
Utilise uniquement requests (pas de Playwright) - très rapide et robuste.
"""

import re
import logging
import time
import sqlite3
import json
import pandas as pd
from pathlib import Path
from typing import List, Dict, Set, Optional
from datetime import datetime
from urllib.parse import urljoin
from html import unescape

import requests
from bs4 import BeautifulSoup

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
BASE_URL = "https://recrutement.bpce.fr"
API_URL = "https://recrutement.bpce.fr/app/wp-json/bpce/v1/search/jobs"

# ================= Config =================
class Config:
    REQUEST_TIMEOUT = 90
    BASE_DIR = Path(__file__).parent
    DB_PATH = BASE_DIR / "bpce_jobs.db"
    CSV_PATH = BASE_DIR / "bpce_jobs.csv"

config = Config()

# ================= Brand → Maison (big picture) =================
# Mapping des marques API vers des noms lisibles pour les vignettes.
# Ordre: marques spécifiques d'abord, puis préfixes génériques.
BRAND_TO_HOUSE = [
    # Maisons spécifiques (exact ou préfixe)
    ("Banque Palatine", "Banque Palatine"),
    ("AEW", "AEW"),
    ("Groupe Crédit Coopératif", "Crédit Coopératif"),
    ("Crédit Coopératif", "Crédit Coopératif"),
    ("ONEY", "Oney"),
    ("Oney", "Oney"),
    ("Mirova", "Mirova"),
    ("Ostrum Asset Management", "Ostrum"),
    ("Ostrum", "Ostrum"),
    ("Crédit Foncier", "Crédit Foncier"),
    ("Capitole Finance", "Capitole Finance"),
    ("Casden", "Casden"),
    # Natixis (toutes variantes CIB, IM, Wealth, etc.)
    ("Natixis", "Natixis"),
    ("NIMI", "Natixis"),
    # Caisse d'Épargne (toutes régionales)
    ("Caisse d'Epargne", "Caisse d'Épargne"),
    ("Caisse d'Épargne", "Caisse d'Épargne"),
    # Banque Populaire (toutes régionales + BRED, Banque BCP, Banque de Savoie)
    ("BRED Banque Populaire", "Banque Populaire"),
    ("Banque Populaire", "Banque Populaire"),
    ("Banque BCP", "Banque Populaire"),
    ("Banque de Savoie", "Banque Populaire"),
    # BPCE (entités centrales, solutions, assurances, etc.)
    ("BPCE", "BPCE"),
]
DEFAULT_HOUSE = "BPCE"


def resolve_company_name(brand_list: List[str]) -> str:
    """Retourne le nom de la maison (big picture) à partir de la liste des marques."""
    if not brand_list:
        return DEFAULT_HOUSE
    brand_raw = (brand_list[0] or "").strip() if brand_list else ""
    if not brand_raw:
        return DEFAULT_HOUSE
    brand_lower = brand_raw.lower()
    for pattern, house in BRAND_TO_HOUSE:
        if pattern.lower() in brand_lower:
            return house
    return DEFAULT_HOUSE


# ================= Contract type mapping =================
CONTRACT_MAPPING = {
    "cdi": "CDI",
    "cdd": "CDD",
    "stage": "Stage",
    "stage-sup-a-2-mois": "Stage",
    "contrat-dapprentissage": "Alternance / Apprentissage",
    "contrat-de-professionnalisation": "Alternance / Apprentissage",
    "contrat-en-alternance": "Alternance / Apprentissage",
    "vie": "VIE",
    "Contrat en alternance": "Alternance / Apprentissage",
}


def normalize_contract(raw: Optional[str]) -> Optional[str]:
    """Normalise le type de contrat."""
    if not raw:
        return None
    if isinstance(raw, list):
        raw = raw[0] if raw else ""
    cleaned = str(raw).strip().lower()
    for key, value in CONTRACT_MAPPING.items():
        if key in cleaned:
            return value
    return raw.strip() if raw else None


def html_to_text(html: Optional[str]) -> str:
    """Convertit le HTML en texte brut."""
    if not html:
        return ""
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text(separator=" ", strip=True)
    return unescape(text)


def build_location(loc_data: Dict) -> Optional[str]:
    """Construit 'Ville - Pays' à partir de localisations API."""
    city_raw = loc_data.get("city") or loc_data.get("localisation") or ""
    region = loc_data.get("region") or ""
    country_raw = loc_data.get("country") or region or ""

    # Utiliser region comme pays si country est "International"
    if str(country_raw).lower() == "international" and region:
        country_raw = region

    city = normalize_city(city_raw) if city_raw else None
    country = normalize_country(country_raw) if country_raw else None

    if not country and city:
        country = get_country_from_city(city) or normalize_country("France")
        if country:
            country = normalize_country(country)

    if city and country and city.lower() != country.lower():
        return f"{city} - {country}"
    elif country:
        return country
    elif city:
        return city
    return None


# =========================================================
# DATABASE MANAGER
# =========================================================
class JobDatabase:
    """Gestion de la base de données SQLite pour BPCE"""

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
    """Extrait le niveau d'étude du texte."""
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
    """Extrait le niveau d'expérience du texte."""
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
# FETCH ALL JOBS FROM API
# =========================================================
def fetch_all_jobs_from_api() -> List[Dict]:
    """Récupère toutes les offres via l'API en une requête."""
    payload = {
        "lang": "fr",
        "keyword": "",
        "tax_sector": "",
        "tax_contract": "",
        "tax_place": "",
        "tax_job": "",
        "tax_experience": "",
        "tax_degree": "",
        "tax_brands": "",
        "tax_department": "",
        "tax_city": "",
        "tax_country": "",
        "tax_channel": "",
        "jobcode": "",
        "tax_community_job": "",
        "external": False,
        "userID": "",
        "from": 0,
        "size": 3000,
    }
    headers = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)"}
    r = requests.post(API_URL, json=payload, headers=headers, timeout=config.REQUEST_TIMEOUT)
    r.raise_for_status()
    data = r.json()
    items = data.get("data", {}).get("items", [])
    total = data.get("data", {}).get("total", 0)
    logging.info(f"API: {len(items)} offres reçues (total: {total})")
    return items


# =========================================================
# TRANSFORM API ITEM TO JOB DICT
# =========================================================
def transform_api_item_to_job(item: Dict) -> Dict:
    """Transforme un item API en dict job pour la base."""
    link = item.get("link", {}) or {}
    link_url = link.get("url", "")
    job_url = urljoin(BASE_URL, link_url) if link_url else ""

    job_id = "BPCE_" + str(item.get("job_number") or item.get("advert_id") or item.get("post_id") or "")

    # Titre
    job_title = item.get("title", "").strip() or None

    # Contrat
    contract_list = item.get("contract", [])
    contract_raw = contract_list[0] if contract_list else None
    contract_type = normalize_contract(contract_raw)

    # Date de publication
    pub_date = item.get("date") or item.get("datetime", "")
    if pub_date and len(str(pub_date)) >= 10:
        pub_date = str(pub_date)[:10]
    else:
        pub_date = None

    # Localisation - prendre la première localisation détaillée
    localisations = item.get("localisations", [])
    location = None
    if localisations:
        loc = localisations[0]
        if isinstance(loc, dict):
            location = build_location(loc)
    if not location and item.get("localisation"):
        # Fallback: localisation brute "Ville" ou "Ville, Pays"
        loc_raw = str(item.get("localisation", ""))
        if "," in loc_raw:
            parts = [p.strip() for p in loc_raw.split(",", 1)]
            city = normalize_city(parts[0]) if parts else None
            country = normalize_country(parts[1]) if len(parts) > 1 else None
            if city and country:
                location = f"{city} - {country}"
            elif country:
                location = country
        else:
            city = normalize_city(loc_raw)
            country = get_country_from_city(city) if city else None
            if city and country:
                location = f"{city} - {normalize_country(country)}"
            elif city:
                location = city

    # Description (HTML → texte)
    desc_html = item.get("description", "")
    job_description = html_to_text(desc_html)
    if job_description:
        job_description = re.sub(r'\s+', ' ', job_description).strip()[:25000]

    # Job family
    job_family = classify_job_family(job_title or "", job_description or "")

    # Education & Experience
    education_level = extract_education_level(job_description)
    experience_level = extract_experience_level(job_description, contract_type)

    # Maison (AEW, Banque Palatine, Natixis, Caisse d'Épargne, etc.)
    company_name = resolve_company_name(item.get("brand", []))

    return {
        "job_url": job_url,
        "job_id": job_id,
        "job_title": job_title,
        "contract_type": contract_type,
        "publication_date": pub_date,
        "location": location,
        "job_family": job_family,
        "duration": None,
        "management_position": None,
        "status": "Live",
        "education_level": education_level,
        "experience_level": experience_level,
        "training_specialization": None,
        "technical_skills": "[]",
        "behavioral_skills": "[]",
        "tools": None,
        "languages": None,
        "job_description": job_description,
        "company_name": company_name,
        "company_description": None,
        "first_seen": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "last_updated": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }


# =========================================================
# MAIN PIPELINE
# =========================================================
def main():
    start = time.time()
    logging.info("=" * 80)
    logging.info("DÉBUT PIPELINE BPCE JOB SCRAPER")
    logging.info("=" * 80)

    db = JobDatabase(config.DB_PATH)
    logging.info(f"Base de données: {config.DB_PATH}")

    # Étape 1: Récupérer toutes les offres via l'API
    logging.info("\n📋 ÉTAPE 1: Récupération des offres via l'API")
    api_items = fetch_all_jobs_from_api()

    jobs = []
    seen_urls = set()
    for item in api_items:
        job = transform_api_item_to_job(item)
        if job.get("job_url") and job["job_url"] not in seen_urls:
            seen_urls.add(job["job_url"])
            jobs.append(job)

    logging.info(f"Offres transformées: {len(jobs)}")

    all_current_links = {j["job_url"] for j in jobs if j.get("job_url")}

    # Étape 2: Identifier nouveaux et expirés
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

    # Étape 4: Insérer/mettre à jour (tous les jobs pour rafraîchir, ou seulement les nouveaux)
    # On met à jour tous les jobs "live" pour garder les données à jour
    today = datetime.now().strftime("%Y-%m-%d")
    for job in jobs:
        if not job.get("job_url"):
            continue
        existing_date = db.get_existing_publication_date(job["job_url"])
        job["publication_date"] = existing_date if existing_date else (job.get("publication_date") or today)
        db.insert_or_update_job(job)

    logging.info(f"\n✓ {len(jobs)} offres en base (Live)")

    # Étape 5: Export CSV
    logging.info("\n💾 ÉTAPE 5: Export vers CSV")
    db.export_to_csv(config.CSV_PATH)
    logging.info(f"✓ CSV exporté: {config.CSV_PATH}")

    # Statistiques
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
    logging.info(f"Temps écoulé: {elapsed:.2f}s")
    logging.info("=" * 80)


if __name__ == "__main__":
    main()
