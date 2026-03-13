#!/usr/bin/env python
# coding: utf-8

"""
Credit Agricole Job Scraper - Pipeline Optimisé
================================================
Système complet pour scraper et tracker les offres d'emploi du Crédit Agricole
Optimisé pour macOS
"""

import requests
from bs4 import BeautifulSoup
import re
import time
import sqlite3
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Set, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import pandas as pd
from dataclasses import dataclass, asdict
import json
import logging
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Import des normaliseurs partagés
try:
    from city_normalizer import normalize_city
    from country_normalizer import normalize_country
    from experience_extractor import extract_experience_level
except ImportError:
    # Fallback pour exécution directe
    import sys
    sys.path.append(str(Path(__file__).parent))
    from city_normalizer import normalize_city
    from country_normalizer import normalize_country
    from experience_extractor import extract_experience_level

# ============================================================================
# CONFIGURATION
# ============================================================================

@dataclass
class Config:
    """Configuration centralisée du scraper"""
    base_url: str = "https://groupecreditagricole.jobs"
    search_url: str = "https://groupecreditagricole.jobs/fr/nos-offres/page/{}/"
    base_dir: Path = None  # Sera défini dans __post_init__ pour être relatif au script
    db_path: Path = None
    csv_path: Path = None
    max_workers: int = 10
    request_timeout: int = 30
    retry_attempts: int = 3
    delay_between_requests: float = 0.1

    def __post_init__(self):
        # Utiliser le dossier PYTHON comme base_dir (même dossier que le script)
        if self.base_dir is None:
            self.base_dir = Path(__file__).parent
        self.base_dir.mkdir(parents=True, exist_ok=True)
        self.db_path = self.base_dir / "credit_agricole_jobs.db"
        self.csv_path = self.base_dir / "credit_agricole_jobs.csv"

# ============================================================================
# LOGGING SETUP
# ============================================================================

def setup_logging(config: Config):
    """Configure le logging"""
    log_file = config.base_dir / "credit_agricole_scraper.log"
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)

# ============================================================================
# HTTP SESSION WITH RETRY
# ============================================================================

def create_session(config: Config) -> requests.Session:
    """Crée une session HTTP avec retry automatique"""
    session = requests.Session()

    retry_strategy = Retry(
        total=config.retry_attempts,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"]
    )

    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://", adapter)
    session.mount("https://", adapter)

    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
    })

    return session

# ============================================================================
# DATABASE MANAGER
# ============================================================================

class JobDatabase:
    """Gestion de la base de données SQLite"""

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

    def get_existing_urls(self) -> Set[str]:
        """Récupère tous les URLs existants"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute("SELECT job_url FROM jobs WHERE is_valid = 1")
            return {row[0] for row in cursor.fetchall()}

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

    def insert_or_update_job(self, job: Dict):
        """Insert ou update un job"""
        with sqlite3.connect(self.db_path) as conn:
            # Vérifier si le job a du contenu valide
            is_valid = 1 if (job.get('job_id') or job.get('job_title') or job.get('job_description')) else 0

            # Convertir les listes en JSON
            job_data = job.copy()
            for key in ['technical_skills', 'behavioral_skills']:
                if isinstance(job_data.get(key), list):
                    job_data[key] = json.dumps(job_data[key], ensure_ascii=False)

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
                job_data.get('job_url'), job_data.get('job_id'), job_data.get('job_title'),
                job_data.get('contract_type'), job_data.get('publication_date'),
                job_data.get('location'), job_data.get('job_family'), job_data.get('duration'),
                job_data.get('management_position'), job_data.get('status', 'Live'),
                job_data.get('education_level'), job_data.get('experience_level'),
                job_data.get('training_specialization'), job_data.get('technical_skills'),
                job_data.get('behavioral_skills'), job_data.get('tools'),
                job_data.get('languages'), job_data.get('job_description'),
                job_data.get('company_name'), job_data.get('company_description'), is_valid
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

# ============================================================================
# JOB LINK SCRAPER
# ============================================================================

class JobLinkScraper:
    """Scraper pour récupérer tous les liens de jobs"""

    def __init__(self, config: Config, session: requests.Session, logger: logging.Logger):
        self.config = config
        self.session = session
        self.logger = logger

    def get_total_jobs_count(self) -> int:
        """Récupère le nombre total d'offres"""
        try:
            response = self.session.get(
                self.config.search_url.format(1),
                timeout=self.config.request_timeout
            )
            response.raise_for_status()

            soup = BeautifulSoup(response.text, "html.parser")
            h2_element = soup.find("h2", class_="js-searchOffersResults")

            if h2_element:
                text = h2_element.get_text(strip=True)
                match = re.search(r"(\d[\d\s]*)", text)
                if match:
                    count = int(match.group(1).replace(" ", ""))
                    self.logger.info(f"Total d'offres disponibles: {count}")
                    return count

            return 0
        except Exception as e:
            self.logger.error(f"Erreur lors de la récupération du compte: {e}")
            return 0

    def get_last_page_number(self) -> int:
        """Récupère le numéro de la dernière page"""
        try:
            response = self.session.get(
                self.config.search_url.format(1),
                timeout=self.config.request_timeout
            )
            response.raise_for_status()

            soup = BeautifulSoup(response.text, "html.parser")
            page_numbers = [
                int(a["data-page"])
                for a in soup.find_all("a", class_="folio-item", href=True)
                if a.get("data-page") and a.get("data-page").isdigit()
            ]

            last_page = max(page_numbers) if page_numbers else 1
            self.logger.info(f"Dernière page détectée: {last_page}")
            return last_page
        except Exception as e:
            self.logger.error(f"Erreur lors de la détection de la dernière page: {e}")
            return 1

    def scrape_page(self, page_num: int) -> Set[str]:
        """Scrape une page pour récupérer les liens"""
        try:
            url = self.config.search_url.format(page_num)
            response = self.session.get(url, timeout=self.config.request_timeout)

            if response.status_code != 200:
                return set()

            soup = BeautifulSoup(response.text, "html.parser")
            links = set()

            for a in soup.find_all("a", href=True):
                href = a["href"]
                if "/nos-offres-emploi/" in href:
                    if href.startswith("/"):
                        href = self.config.base_url + href
                    links.add(href)

            time.sleep(self.config.delay_between_requests)
            return links

        except Exception as e:
            self.logger.error(f"Erreur page {page_num}: {e}")
            return set()

    def scrape_all_links(self) -> Set[str]:
        """Scrape tous les liens de jobs"""
        total_count = self.get_total_jobs_count()
        last_page = self.get_last_page_number()

        all_links = set()

        self.logger.info(f"Début du scraping des liens ({last_page} pages)")

        with tqdm(total=total_count, desc="🔄 Collecte des liens", unit="job") as pbar:
            for page in range(1, last_page + 1):
                page_links = self.scrape_page(page)
                new_links = page_links - all_links
                all_links.update(new_links)
                pbar.update(len(new_links))

        self.logger.info(f"Total de liens collectés: {len(all_links)}")
        return all_links

# ============================================================================
# JOB DETAIL SCRAPER
# ============================================================================

class JobDetailScraper:
    """Scraper pour récupérer les détails d'un job (sans Selenium!)"""

    def __init__(self, config: Config, session: requests.Session, logger: logging.Logger):
        self.config = config
        self.session = session
        self.logger = logger

    def clean_text(self, text: str) -> str:
        """Nettoie le texte"""
        if not text:
            return ""
        return text.strip().replace("\n", " ").replace("\r", " ")

    def normalize_location(self, location: str) -> str:
        """Normalise le format de la localisation en 'Ville - Pays' en utilisant les normaliseurs partagés"""
        if not location:
            return ""

        # Nettoyage initial
        location = location.strip().strip('"').strip()
        location = re.sub(r'\s+', ' ', location)
        location = re.sub(r'^Lieu\s*:\s*', '', location, flags=re.IGNORECASE)

        city_raw = ""
        country_raw = ""

        if " - " in location:
            parts = [p.strip() for p in location.split(" - ")]
            if len(parts) >= 2:
                city_raw = parts[0]
                country_raw = parts[-1]

                # Si city_raw contient des virgules, c'est souvent au format complexe
                # Ex: "Crédit Agricole Merca Leasing GmbH & Co. KG, Westerbachstraße 28, 61476 Kronberg / Taunus"
                # Ex: "Crédit Agricole Leasing & Factoring, Einsteinring 30, 85609 Aschheim"
                # Ex: "METRO PARK, NEW JERSEY" -> on veut "NEW JERSEY"
                if "," in city_raw:
                    city_parts = [p.strip() for p in city_raw.split(",")]
                    
                    # IMPORTANT : Détecter et rejeter les noms d'entreprises
                    # Ex: "Crédit Agricole Merca Leasing GmbH & Co. KG"
                    # Patterns d'entreprises: GmbH, & Co., KG, S.A., Leasing, Factoring, etc.
                    company_keywords = [
                        'gmbh', '& co.', '& co', 'kg', 's.a.', 'sa', 
                        'leasing', 'factoring', 'ltd', 'llc', 'inc',
                        'crédit agricole', 'credit agricole'
                    ]
                    
                    # Filtrer les parties qui sont des noms d'entreprises
                    valid_parts = []
                    for part in city_parts:
                        part_lower = part.lower()
                        # Vérifier si la partie contient un mot-clé d'entreprise
                        is_company = any(keyword in part_lower for keyword in company_keywords)
                        if not is_company:
                            valid_parts.append(part)
                    
                    # Si on a des parties valides, travailler avec elles
                    if valid_parts:
                        city_parts = valid_parts
                    
                    # Prendre la dernière partie non-entreprise
                    city_raw = city_parts[-1] if city_parts else ""

                # IMPORTANT : Extraire la ville des formats avec code postal
                # Ex: "61476 Kronberg / Taunus" -> "Kronberg"
                # Ex: "85609 Aschheim" -> "Aschheim"
                # Pattern: code postal (4-5 chiffres) suivi de la ville
                postal_match = re.match(r'^\d{4,5}\s+(.+?)(?:\s*/\s*\w+)?$', city_raw)
                if postal_match:
                    city_raw = postal_match.group(1).strip()
                    # Si la ville contient " / ", prendre la partie avant le slash
                    # Ex: "Kronberg / Taunus" -> "Kronberg"
                    if " / " in city_raw:
                        city_raw = city_raw.split(" / ")[0].strip()

                # IMPORTANT : Détecter et rejeter les adresses avec numéro + rue
                # Ex: "9 Allée Scheffer", "168 Robinson Road", "Einsteinring 30"
                address_patterns = [
                    r'^\d+\s+(allée|allee|avenue|rue|road|street|boulevard|chemin|einsteinring|westerbachstraße|westerbachstrasse)',
                ]
                is_address = False
                for pattern in address_patterns:
                    if re.match(pattern, city_raw.lower()):
                        is_address = True
                        break
                
                if is_address:
                    # C'est une adresse, pas une ville - chercher dans les parties précédentes
                    if "," in parts[0]:
                        city_parts = [p.strip() for p in parts[0].split(",")]
                        # Chercher la dernière partie qui contient un code postal + ville
                        for part in reversed(city_parts):
                            postal_match = re.match(r'^\d{4,5}\s+(.+?)(?:\s*/\s*\w+)?$', part)
                            if postal_match:
                                city_raw = postal_match.group(1).strip()
                                if " / " in city_raw:
                                    city_raw = city_raw.split(" / ")[0].strip()
                                break
                        else:
                            # Aucun code postal trouvé, rejeter
                            city_raw = ""
                    else:
                        city_raw = ""  # Rejeter complètement
                
                city_raw = city_raw.strip()
            else:
                city_raw = parts[0]
        else:
            # 1. Détection par parenthèses (format fréquent chez CA : "Ville (Pays)")
            paren_match = re.search(r'\((.*?)\)', location)
            if paren_match:
                country_candidate = paren_match.group(1).strip()
                # On nettoie la ville en retirant les parenthèses
                city_raw = re.sub(r'\(.*?\)', '', location).strip()
                country_raw = country_candidate
            else:
                # 2. Détection par mots-clés
                known_countries = [
                    "France", "Italie", "Allemagne", "Luxembourg", "Suisse", "Pays-Bas", 
                    "États-Unis", "Canada", "Singapour", "Japon", "Royaume-Uni", "United Kingdom",
                    "Maroc", "Tunisie", "Algérie", "Belgique", "Espagne", "Portugal", "Irlande"
                ]
                found_country = None
                for c in known_countries:
                    if c.lower() in location.lower():
                        found_country = c
                        break
                
                if found_country:
                    country_raw = found_country
                    city_raw = location.replace(found_country, "").strip()
                else:
                    # Défaut France si rien trouvé
                    city_raw = location
                    country_raw = "France"

        # Appliquer les normaliseurs partagés
        city = normalize_city(city_raw)
        country = normalize_country(country_raw)

        # Si la ville est None (rejetée par le normaliseur) ou égale au pays, afficher uniquement le pays
        if not city or city.lower() == country.lower():
            return country
        
        return f"{city} - {country}"

    def normalize_education_level(self, education: str) -> str:
        """Harmonise les niveaux d'études"""
        if not education:
            return ""

        education = education.strip()

        # Mapping pour harmoniser les variantes
        education_mapping = {
            "Master": "Bac + 5 / M2 et plus",
            "Master 2": "Bac + 5 / M2 et plus",
            "M2": "Bac + 5 / M2 et plus",
            "Certificat Fédéral de Capacité": "Bac + 2 / L2",
            "Certificat  Fédéral de Capacité": "Bac + 2 / L2",
            "CFC": "Bac + 2 / L2",
            "Bachelor": "Bac + 3 / L3",
            "Licence": "Bac + 3 / L3",
            "L3": "Bac + 3 / L3",
        }

        # Chercher une correspondance
        for key, value in education_mapping.items():
            if key.lower() == education.lower():
                return value

        # Si déjà dans un format standard, retourner tel quel
        return education

    def check_if_exists(self, url: str) -> bool:
        """Vérifie si la page existe (détection 404)"""
        try:
            response = self.session.head(url, timeout=10, allow_redirects=True)
            return response.status_code == 200
        except:
            return False

    def scrape_job(self, url: str) -> Optional[Dict]:
        """Scrape les détails d'un job"""
        try:
            # Vérification rapide de l'existence
            if not self.check_if_exists(url):
                self.logger.warning(f"Page non trouvée (404): {url}")
                return None

            response = self.session.get(url, timeout=self.config.request_timeout)
            response.raise_for_status()

            soup = BeautifulSoup(response.text, "html.parser")

            job = {
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
                "company_name": "",
                "company_description": "",
                "job_url": url
            }

            # Extraction des informations de base
            for li in soup.find_all("li", class_=lambda c: c and ("offer-location" in c or "offer-job" in c or "offer-ref" in c)):
                cls = li.get("class", [])
                text = self.clean_text(li.get_text())

                if any("offer-location" in c for c in cls):
                    job["location"] = self.normalize_location(text)
                elif any("offer-job" in c for c in cls):
                    job["job_family"] = text
                elif any("offer-ref" in c for c in cls):
                    job["job_id"] = text

            # Titre
            title = soup.find("h1", class_="offer-title")
            if title:
                job["job_title"] = self.clean_text(title.get_text())

            # Type de contrat
            contract = soup.find("div", class_="tag")
            if contract:
                span = contract.find("span")
                if span:
                    job["contract_type"] = self.clean_text(span.get_text())

            # Date de publication
            pub = soup.find("p", class_="publication-date")
            if pub:
                job["publication_date"] = self.clean_text(pub.get_text().replace("Modifiée le", ""))

            # Informations détaillées
            for dt in soup.find_all("dt", class_="information-title"):
                try:
                    dd = dt.find_next_sibling("dd")
                    if not dd:
                        continue

                    key = self.clean_text(dt.get_text()).lower()
                    value = self.clean_text(dd.get_text())

                    if "durée" in key:
                        job["duration"] = value
                    elif "management" in key:
                        job["management_position"] = value
                    elif "niveau d'étude" in key or "niveau d'études" in key:
                        job["education_level"] = value
                    elif "niveau d'expérience" in key:
                        job["experience_level"] = value
                    elif "outils informatiques" in key:
                        job["tools"] = value
                    elif "langues" in key:
                        job["languages"] = value
                    elif "formation" in key or "spécialisation" in key:
                        job["training_specialization"] = value
                    elif "compétences recherchées" in key:
                        skills = [self.clean_text(li.get_text()) for li in dd.find_all("li")]
                        job["behavioral_skills"] = skills
                    elif "compétences clés" in key or "compétences techniques" in key:
                        skills = [self.clean_text(li.get_text()) for li in dd.find_all("li")]
                        job["technical_skills"] = skills
                except Exception as e:
                    continue

            # Texte complet de l'offre pour la recherche par mots-clés (non affiché sur les vignettes)
            main_content = soup.find("main") or soup.find("div", class_=lambda c: c and "offer" in (c or ""))
            if main_content:
                full_text = self.clean_text(main_content.get_text())
                # Limiter à 25k caractères pour stockage raisonnable tout en gardant le maximum pour la recherche
                job["job_description"] = full_text[:25000] if full_text else ""
            if not job.get("job_description") or not job["job_description"].strip():
                desc = soup.find("section", class_="offer-content")
                if desc:
                    job["job_description"] = self.clean_text(desc.get_text())

            # Nom de l'entreprise
            company = soup.find("h1", class_="entity-name")
            if company:
                job["company_name"] = self.clean_text(company.get_text())

            # Description de l'entreprise
            company_desc = soup.find("div", class_="accordion-item-content")
            if company_desc:
                job["company_description"] = self.clean_text(company_desc.get_text())

            # Fallback : si expérience non trouvée dans dt/dd, extraire depuis company_description ou job_description
            if not job.get("experience_level"):
                combined = " ".join(filter(None, [
                    job.get("company_description", ""),
                    job.get("job_description", "")
                ]))
                if combined:
                    job["experience_level"] = extract_experience_level(
                        combined, job.get("contract_type"), job.get("job_title")
                    )

            return job

        except Exception as e:
            self.logger.error(f"Erreur scraping {url}: {e}")
            return None

# ============================================================================
# MAIN PIPELINE
# ============================================================================

class CreditAgricoleJobPipeline:
    """Pipeline principal pour orchestrer tout le scraping"""

    def __init__(self, config: Config = None):
        self.config = config or Config()
        self.logger = setup_logging(self.config)
        self.session = create_session(self.config)
        self.db = JobDatabase(self.config.db_path)
        self.link_scraper = JobLinkScraper(self.config, self.session, self.logger)
        self.detail_scraper = JobDetailScraper(self.config, self.session, self.logger)

    def run(self):
        """Exécute le pipeline complet"""
        self.logger.info("=" * 80)
        self.logger.info("DÉBUT DU PIPELINE CRÉDIT AGRICOLE JOB SCRAPER")
        self.logger.info("=" * 80)

        # Étape 1: Collecter tous les liens
        self.logger.info("\n📋 ÉTAPE 1: Collection des liens d'offres")
        all_current_links = self.link_scraper.scrape_all_links()

        # Étape 2: Identifier les nouveaux et les expirés
        self.logger.info("\n🔍 ÉTAPE 2: Analyse des changements")
        existing_live_urls = self.db.get_live_urls()

        new_urls = all_current_links - existing_live_urls
        expired_urls = existing_live_urls - all_current_links

        self.logger.info(f"✅ Nouvelles offres: {len(new_urls)}")
        self.logger.info(f"❌ Offres expirées: {len(expired_urls)}")

        # Étape 3: Marquer les expirées
        if expired_urls:
            self.logger.info("\n⏳ ÉTAPE 3: Marquage des offres expirées")
            self.db.mark_as_expired(expired_urls)
            self.logger.info(f"✓ {len(expired_urls)} offres marquées comme expirées")

        # Étape 4: Scraper les nouveaux détails
        if new_urls:
            self.logger.info(f"\n🚀 ÉTAPE 4: Scraping de {len(new_urls)} nouvelles offres")
            self.scrape_jobs_parallel(sorted(new_urls))
        else:
            self.logger.info("\n✓ Aucune nouvelle offre à scraper")

        # Étape 5: Export CSV
        self.logger.info("\n💾 ÉTAPE 5: Export vers CSV")
        self.db.export_to_csv(self.config.csv_path)
        self.logger.info(f"✓ CSV exporté: {self.config.csv_path}")

        # Statistiques finales
        self.print_final_stats()

        self.logger.info("\n" + "=" * 80)
        self.logger.info("✅ PIPELINE TERMINÉ AVEC SUCCÈS")
        self.logger.info("=" * 80)

    def scrape_jobs_parallel(self, urls: List[str]):
        """Scrape les jobs en parallèle"""
        successful = 0
        failed = 0

        with ThreadPoolExecutor(max_workers=self.config.max_workers) as executor:
            futures = {executor.submit(self.detail_scraper.scrape_job, url): url for url in urls}

            with tqdm(total=len(urls), desc="🔄 Scraping détails", unit="job") as pbar:
                for future in as_completed(futures):
                    try:
                        job_data = future.result()
                        if job_data:
                            self.db.insert_or_update_job(job_data)
                            successful += 1
                        else:
                            failed += 1
                    except Exception as e:
                        self.logger.error(f"Erreur: {e}")
                        failed += 1

                    pbar.update(1)

        self.logger.info(f"✓ Succès: {successful} | Échecs: {failed}")

    def print_final_stats(self):
        """Affiche les statistiques finales"""
        with sqlite3.connect(self.config.db_path) as conn:
            cursor = conn.execute("""
                SELECT 
                    COUNT(*) as total,
                    SUM(CASE WHEN status = 'Live' THEN 1 ELSE 0 END) as live,
                    SUM(CASE WHEN status = 'Expired' THEN 1 ELSE 0 END) as expired,
                    SUM(CASE WHEN is_valid = 0 THEN 1 ELSE 0 END) as invalid
                FROM jobs
            """)
            stats = cursor.fetchone()

            self.logger.info("\n" + "=" * 60)
            self.logger.info("📊 STATISTIQUES FINALES")
            self.logger.info("=" * 60)
            self.logger.info(f"Total d'offres en base: {stats[0]}")
            self.logger.info(f"  └─ Live (actives): {stats[1]}")
            self.logger.info(f"  └─ Expired (expirées): {stats[2]}")
            self.logger.info(f"  └─ Invalid (pages 404): {stats[3]}")
            self.logger.info("=" * 60)

# ============================================================================
# INTERFACE JUPYTER NOTEBOOK
# ============================================================================

def run_scraper(custom_config: Dict = None):
    """
    Fonction principale à appeler depuis Jupyter Notebook

    Args:
        custom_config: Dictionnaire de configuration personnalisée (optionnel)

    Example:
        run_scraper()

        # Ou avec config personnalisée:
        run_scraper({
            'max_workers': 15,
            'base_dir': Path('/custom/path')
        })
    """
    config = Config()

    if custom_config:
        for key, value in custom_config.items():
            if hasattr(config, key):
                setattr(config, key, value)

    pipeline = CreditAgricoleJobPipeline(config)
    pipeline.run()

    return pipeline

# ============================================================================
# EXEMPLE D'UTILISATION
# ============================================================================

if __name__ == "__main__":
    # Exécution simple
    run_scraper()

    # Ou avec configuration personnalisée:
    # run_scraper({
    #     'max_workers': 15,
    #     'delay_between_requests': 0.05
    # })


