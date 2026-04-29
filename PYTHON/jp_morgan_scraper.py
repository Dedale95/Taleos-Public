#!/usr/bin/env python3
"""
JP MORGAN CHASE — JOB SCRAPER
Extrait les offres d'emploi de JP Morgan via l'API Oracle HCM REST.
Phase 1 : API REST (rapide, ~40 requêtes pour ~7500 offres).
Phase 2 : Playwright async pour les descriptions complètes (nouvelles offres uniquement).
"""

import asyncio
import json
import logging
import re
import sqlite3
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Set

import requests
from bs4 import BeautifulSoup

try:
    from playwright.async_api import async_playwright, BrowserContext
    PLAYWRIGHT_AVAILABLE = True
except ImportError:
    PLAYWRIGHT_AVAILABLE = False
    logging.warning("playwright non disponible — les descriptions ne seront pas scrappées")

try:
    from city_normalizer import normalize_city
    from country_normalizer import normalize_country
    from job_family_classifier import classify_job_family
    from experience_extractor import extract_experience_level
except ImportError:
    import sys
    sys.path.append(str(Path(__file__).parent))
    from city_normalizer import normalize_city
    from country_normalizer import normalize_country
    from job_family_classifier import classify_job_family
    from experience_extractor import extract_experience_level

# ─────────────────────────── Logging ────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# ─────────────────────────── Constants ──────────────────────────
SITE_NUMBER   = "CX_1001"
BASE_API      = "https://jpmc.fa.oraclecloud.com/hcmRestApi/resources/latest/recruitingCEJobRequisitions"
DETAIL_URL    = "https://jpmc.fa.oraclecloud.com/hcmUI/CandidateExperience/en/sites/CX_1001/job/{job_id}"
COMPANY_NAME  = "JP Morgan Chase"

BATCH_SIZE    = 200   # maximum supporté par l'API
CONCURRENCY   = 10    # Playwright : pages simultanées pour les détails
REQUEST_TIMEOUT = 30  # seconds
PAGE_TIMEOUT  = 30_000  # ms (Playwright)

# ─────────────────────────── Config ─────────────────────────────
class Config:
    BASE_DIR  = Path(__file__).parent
    DB_PATH   = BASE_DIR / "jp_morgan_jobs.db"
    CSV_PATH  = BASE_DIR / "jp_morgan_jobs.csv"
    HEADLESS  = True

# ─────────────── ISO country code → French name ─────────────────
ISO_TO_FRENCH: Dict[str, str] = {
    "US": "États-Unis",
    "GB": "Royaume-Uni",
    "FR": "France",
    "DE": "Allemagne",
    "ES": "Espagne",
    "IT": "Italie",
    "NL": "Pays-Bas",
    "BE": "Belgique",
    "CH": "Suisse",
    "AT": "Autriche",
    "PL": "Pologne",
    "CZ": "Tchéquie",
    "RO": "Roumanie",
    "HU": "Hongrie",
    "PT": "Portugal",
    "GR": "Grèce",
    "DK": "Danemark",
    "SE": "Suède",
    "NO": "Norvège",
    "FI": "Finlande",
    "IE": "Irlande",
    "RU": "Russie",
    "TR": "Turquie",
    "LU": "Luxembourg",
    "MC": "Monaco",
    "GG": "Guernesey",
    "HK": "Hong-Kong",
    "SG": "Singapour",
    "JP": "Japon",
    "CN": "Chine",
    "IN": "Inde",
    "AU": "Australie",
    "NZ": "Nouvelle-Zélande",
    "KR": "Corée du Sud",
    "TW": "Taïwan",
    "MY": "Malaisie",
    "TH": "Thaïlande",
    "VN": "Vietnam",
    "ID": "Indonésie",
    "PH": "Philippines",
    "AE": "Émirats Arabes Unis",
    "SA": "Arabie Saoudite",
    "QA": "Qatar",
    "BH": "Bahreïn",
    "KW": "Koweït",
    "IL": "Israël",
    "EG": "Égypte",
    "MA": "Maroc",
    "TN": "Tunisie",
    "DZ": "Algérie",
    "ZA": "Afrique du Sud",
    "NG": "Nigeria",
    "KE": "Kenya",
    "DJ": "Djibouti",
    "BR": "Brésil",
    "AR": "Argentine",
    "CL": "Chili",
    "MX": "Mexique",
    "CO": "Colombie",
    "CA": "Canada",
    "MX": "Mexique",
    "PA": "Panama",
    "PL": "Pologne",
    "UA": "Ukraine",
    "CY": "Chypre",
    "MT": "Malte",
    "HR": "Croatie",
    "SK": "Slovaquie",
    "SI": "Slovénie",
    "BG": "Bulgarie",
    "RS": "Serbie",
    "BA": "Bosnie-Herzégovine",
    "MK": "Macédoine",
    "AL": "Albanie",
    "LT": "Lituanie",
    "LV": "Lettonie",
    "EE": "Estonie",
    "BY": "Biélorussie",
}

# ─────────────── Oracle JobFamily → Taleos job family ───────────
ORACLE_FAMILY_MAP: Dict[str, str] = {
    # Technologie
    "Software Engineering":         "IT, Digital et Data",
    "Technology":                   "IT, Digital et Data",
    "Data & Analytics":             "IT, Digital et Data",
    "Information Technology":       "IT, Digital et Data",
    "Cybersecurity":                "IT, Digital et Data",
    "Infrastructure":               "IT, Digital et Data",
    "Cloud":                        "IT, Digital et Data",
    "Machine Learning & AI":        "IT, Digital et Data",
    # Commercial / Clients
    "Client Advisory":              "Commercial / Relations Clients",
    "Client Management":            "Commercial / Relations Clients",
    "Relationship Management":      "Commercial / Relations Clients",
    "Sales":                        "Commercial / Relations Clients",
    "Advisors":                     "Commercial / Relations Clients",
    "Wealth Management":            "Commercial / Relations Clients",
    "Private Banking":              "Commercial / Relations Clients",
    # Finance / Investissement
    "Investment Banking":           "Financement et Investissement",
    "Capital Markets":              "Financement et Investissement",
    "Trading":                      "Financement et Investissement",
    "Markets":                      "Financement et Investissement",
    "Asset Management":             "Financement et Investissement",
    "Corporate Finance":            "Financement et Investissement",
    "Originations":                 "Financement et Investissement",
    "Lending":                      "Financement et Investissement",
    # Risques / Conformité
    "Risk":                         "Risques / Contrôles permanents",
    "Risk Management":              "Risques / Contrôles permanents",
    "Credit Risk":                  "Risques / Contrôles permanents",
    "Compliance":                   "Conformité / Sécurité financière",
    "Legal":                        "Juridique",
    "Audit":                        "Audit / Inspection",
    # Finance / Compta
    "Finance":                      "Finances / Comptabilité / Contrôle de gestion",
    "Accounting":                   "Finances / Comptabilité / Contrôle de gestion",
    "Controllers":                  "Finances / Comptabilité / Contrôle de gestion",
    "Treasury":                     "Finances / Comptabilité / Contrôle de gestion",
    # Opérations
    "Operations":                   "Gestion des opérations",
    "Transaction Processing":       "Gestion des opérations",
    "Back Office":                  "Gestion des opérations",
    "Middle Office":                "Gestion des opérations",
    # RH / Support
    "Human Resources":              "RH / Formation / Communication",
    "Communications":               "RH / Formation / Communication",
    "Marketing":                    "RH / Formation / Communication",
    "Project Management":           "Organisation / Projet / PMO",
    "Strategy":                     "Organisation / Projet / PMO",
    "Business Management":          "Organisation / Projet / PMO",
    # Autres
    "Retail Banking":               "Commercial / Relations Clients",
    "Consumer Banking":             "Commercial / Relations Clients",
    # Familles Oracle spécifiques JP Morgan
    "Associate Bankers":            "Commercial / Relations Clients",
    "Private Client Bankers":       "Commercial / Relations Clients",
    "Client Service":               "Commercial / Relations Clients",
    "Relationship Manager":         "Commercial / Relations Clients",
    "Client Solutions":             "Commercial / Relations Clients",
    "Client Sales":                 "Commercial / Relations Clients",
    "Customer Success":             "Commercial / Relations Clients",
    "Banking":                      "Commercial / Relations Clients",
    "Private Banking":              "Commercial / Relations Clients",
    "Product Management":           "Organisation / Projet / PMO",
    "Product Portfolio & Delivery": "Organisation / Projet / PMO",
    "Technical Program Delivery":   "Organisation / Projet / PMO",
    "Project Management":           "Organisation / Projet / PMO",
    "Product Development":          "IT, Digital et Data",
    "User Experience Design":       "IT, Digital et Data",
    "Architecture":                 "IT, Digital et Data",
    "Predictive Science":           "IT, Digital et Data",
    "Technology Support":           "IT, Digital et Data",
    "Control Officers":             "Risques / Contrôles permanents",
    "Administrative Assistant":     "RH / Formation / Communication",
    "Administrative":               "RH / Formation / Communication",
    "Analysts":                     "Financement et Investissement",
    "Associates":                   "Financement et Investissement",
    "Account Service":              "Commercial / Relations Clients",
    "Client Service Delivery":      "Commercial / Relations Clients",
    "Branch Field Management":      "Commercial / Relations Clients",
    "Sales Support":                "Commercial / Relations Clients",
    "Portfolio Management":         "Financement et Investissement",
    "Financial Analysis":           "Finances / Comptabilité / Contrôle de gestion",
    "Trusts & Estates":             "Financement et Investissement",
    "Program & Project Management": "Organisation / Projet / PMO",
    "Security & Life Safety":       "Risques / Contrôles permanents",
    "Seasonal Employee":            "Commercial / Relations Clients",
}

# ─────────────── Contract type inference from title ──────────────
_CONTRACT_PATTERNS: List[tuple] = [
    # Stage / Internship
    (re.compile(r'\bintern\b|\binternship\b|\bstagiaire\b|\bstage\b', re.I),
     "Stage"),
    (re.compile(r'\bsummer\s+(analyst|associate|intern)\b', re.I),
     "Stage"),
    (re.compile(r'\bgraduate\s+(analyst|associate|trainee|program)\b', re.I),
     "Stage"),
    (re.compile(r'\btrainee\b|\bgraduate\s+scheme\b|\bplacement\b', re.I),
     "Stage"),
    (re.compile(r'\bjunior\s+analyst\b.*\bprogram\b', re.I),
     "Stage"),
    # Alternance / Apprenticeship
    (re.compile(r'\bapprentice\b|\bapprentissage\b|\balternance\b|\balternant\b', re.I),
     "Alternance / Apprentissage"),
    # CDD
    (re.compile(r'\bfixed[\s-]?term\b|\btemporar\w*\b|\bcontract\s+role\b', re.I),
     "CDD"),
    # VIE
    (re.compile(r'\bVIE\b|\bvolontariat\b', re.I),
     "VIE"),
]

# ────────────────────────── Helpers ─────────────────────────────

def infer_contract_type(title: str) -> str:
    """Infère le type de contrat depuis le titre de l'offre. CDI par défaut."""
    for pattern, contract in _CONTRACT_PATTERNS:
        if pattern.search(title):
            return contract
    return "CDI"


def parse_location(primary_location: str, iso_country: str) -> str:
    """
    Convertit le format Oracle "City, State, Country" → "Ville - Pays (FR)".
    Exemples :
      "San Francisco, CA, United States" → "San Francisco - États-Unis"
      "Paris, Île-de-France, France"     → "Paris - France"
      "London, England, United Kingdom"  → "Londres - Royaume-Uni"
      "Hong Kong, , Hong Kong"           → "Hong-Kong"
    """
    if not primary_location:
        country_fr = ISO_TO_FRENCH.get(iso_country or "", "")
        return country_fr or iso_country or ""

    parts = [p.strip() for p in primary_location.split(",")]
    parts = [p for p in parts if p]  # retirer les vides

    if not parts:
        return normalize_country(iso_country) or ""

    # Dernier élément = nom du pays anglais
    country_raw = parts[-1].strip()
    country_fr  = normalize_country(country_raw)
    if not country_fr and iso_country:
        country_fr = ISO_TO_FRENCH.get(iso_country, "")

    # Premier élément = ville (on ignore l'état/région intermédiaire)
    city_raw = parts[0].strip()

    # Correction de noms de villes connus
    CITY_CORRECTIONS: Dict[str, str] = {
        "london": "Londres",
        "new york": "New York",
        "hong kong": "Hong-Kong",
        "singapore": "Singapour",
        "tokyo": "Tokyo",
        "dubai": "Dubaï",
        "zurich": "Zurich",
        "zürich": "Zurich",
        "frankfurt": "Francfort",
        "frankfurt am main": "Francfort",
        "munich": "Munich",
        "glasgow": "Glasgow",
        "edinburgh": "Édimbourg",
    }
    city_fr = CITY_CORRECTIONS.get(city_raw.lower(), city_raw)

    if city_fr and country_fr:
        # Éviter la redondance "Hong-Kong - Hong-Kong"
        if city_fr.lower().replace("-", " ") == country_fr.lower().replace("-", " "):
            return country_fr
        return f"{city_fr} - {country_fr}"
    elif country_fr:
        return country_fr
    elif city_fr:
        return city_fr
    return primary_location


def map_job_family(oracle_family: str, title: str, description: str) -> str:
    """Mappe la famille Oracle vers la taxonomie Taleos."""
    if oracle_family:
        for key, taleos in ORACLE_FAMILY_MAP.items():
            if key.lower() == oracle_family.lower():
                return taleos
    # Fallback : classification par mots-clés depuis titre + description
    return classify_job_family(title, description or "")


def extract_education(text: str) -> Optional[str]:
    """Extrait le niveau d'éducation depuis le texte de l'offre."""
    if not text:
        return None
    text_lower = text.lower()
    patterns = [
        (r"ph\.?d\.?|doctorat|doctorate",                         "Bac + 8 / Doctorat"),
        (r"master|mba|m\.?s\.?\b|bac\s*\+\s*5|grande\s+école|"
         r"engineering\s+school|business\s+school|ingénieur",      "Bac + 5 / M2 et plus"),
        (r"bac\s*\+\s*4|m1",                                       "Bac + 4 / M1"),
        (r"bachelor|bac\s*\+\s*3|licence",                         "Bac + 3 / L3"),
        (r"bac\s*\+\s*2|bts|dut|associate\s+degree",               "Bac + 2 / L2"),
        (r"\bbac\b(?!\s*\+)|\bhigh\s+school\b",                    "Bac"),
    ]
    for pattern, level in patterns:
        if re.search(pattern, text_lower):
            return level
    return None


# ─────────────────────── Database layer ─────────────────────────

class Database:
    def __init__(self, db_path: Path):
        self.db_path = db_path
        self._init()

    def _init(self):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS jobs (
                    job_url              TEXT PRIMARY KEY,
                    job_id               TEXT,
                    job_title            TEXT,
                    contract_type        TEXT,
                    publication_date     TEXT,
                    location             TEXT,
                    job_family           TEXT,
                    duration             TEXT,
                    management_position  TEXT,
                    status               TEXT DEFAULT 'Live',
                    education_level      TEXT,
                    experience_level     TEXT,
                    training_specialization TEXT,
                    technical_skills     TEXT,
                    behavioral_skills    TEXT,
                    tools                TEXT,
                    languages            TEXT,
                    job_description      TEXT,
                    company_name         TEXT,
                    company_description  TEXT,
                    first_seen           TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    last_updated         TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    scrape_attempts      INTEGER DEFAULT 0,
                    is_valid             INTEGER DEFAULT 1
                )
            """)
            conn.commit()

    def get_live_urls(self) -> Set[str]:
        with sqlite3.connect(self.db_path) as conn:
            rows = conn.execute(
                "SELECT job_url FROM jobs WHERE status = 'Live' AND is_valid = 1"
            ).fetchall()
            return {r[0] for r in rows}

    def get_urls_without_description(self) -> Set[str]:
        """URLs des offres Live sans description (pour Phase 2 delta)."""
        with sqlite3.connect(self.db_path) as conn:
            rows = conn.execute(
                "SELECT job_url FROM jobs WHERE status = 'Live' AND is_valid = 1 "
                "AND (job_description IS NULL OR job_description = '')"
            ).fetchall()
            return {r[0] for r in rows}

    def get_existing_date(self, job_url: str) -> Optional[str]:
        with sqlite3.connect(self.db_path) as conn:
            row = conn.execute(
                "SELECT publication_date, first_seen FROM jobs WHERE job_url = ?",
                (job_url,)
            ).fetchone()
            if not row:
                return None
            return row[0] or (str(row[1])[:10] if row[1] else None)

    def mark_expired(self, urls: Set[str]):
        if not urls:
            return
        with sqlite3.connect(self.db_path) as conn:
            placeholders = ",".join("?" * len(urls))
            conn.execute(
                f"UPDATE jobs SET status = 'Expired', last_updated = CURRENT_TIMESTAMP "
                f"WHERE job_url IN ({placeholders})",
                tuple(urls),
            )
            conn.commit()

    def upsert(self, job: Dict):
        """Insert ou met à jour une offre."""
        url = job.get("job_url", "")
        if not url:
            return
        existing_date = self.get_existing_date(url)
        if existing_date:
            job.setdefault("publication_date", existing_date)

        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                INSERT INTO jobs (
                    job_url, job_id, job_title, contract_type, publication_date,
                    location, job_family, duration, management_position, status,
                    education_level, experience_level, training_specialization,
                    technical_skills, behavioral_skills, tools, languages,
                    job_description, company_name, company_description,
                    last_updated, is_valid
                ) VALUES (
                    :job_url, :job_id, :job_title, :contract_type, :publication_date,
                    :location, :job_family, :duration, :management_position, :status,
                    :education_level, :experience_level, :training_specialization,
                    :technical_skills, :behavioral_skills, :tools, :languages,
                    :job_description, :company_name, :company_description,
                    CURRENT_TIMESTAMP, :is_valid
                )
                ON CONFLICT(job_url) DO UPDATE SET
                    job_title           = excluded.job_title,
                    contract_type       = excluded.contract_type,
                    publication_date    = COALESCE(excluded.publication_date, jobs.publication_date),
                    location            = excluded.location,
                    job_family          = excluded.job_family,
                    status              = excluded.status,
                    education_level     = COALESCE(excluded.education_level, jobs.education_level),
                    experience_level    = COALESCE(excluded.experience_level, jobs.experience_level),
                    job_description     = COALESCE(excluded.job_description, jobs.job_description),
                    company_name        = excluded.company_name,
                    last_updated        = CURRENT_TIMESTAMP,
                    is_valid            = excluded.is_valid
            """, {
                "job_url":                url,
                "job_id":                 job.get("job_id", ""),
                "job_title":              job.get("job_title", ""),
                "contract_type":          job.get("contract_type", ""),
                "publication_date":       job.get("publication_date", ""),
                "location":               job.get("location", ""),
                "job_family":             job.get("job_family", ""),
                "duration":               job.get("duration", ""),
                "management_position":    job.get("management_position", ""),
                "status":                 job.get("status", "Live"),
                "education_level":        job.get("education_level", ""),
                "experience_level":       job.get("experience_level", ""),
                "training_specialization": job.get("training_specialization", ""),
                "technical_skills":       job.get("technical_skills", "[]"),
                "behavioral_skills":      job.get("behavioral_skills", "[]"),
                "tools":                  job.get("tools", ""),
                "languages":              job.get("languages", ""),
                "job_description":        job.get("job_description", ""),
                "company_name":           job.get("company_name", COMPANY_NAME),
                "company_description":    job.get("company_description", ""),
                "is_valid":               1,
            })
            conn.commit()

    def upsert_description(self, job_url: str, description: str,
                           education: Optional[str], experience: Optional[str]):
        """Met à jour uniquement la description et les niveaux extraits."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                UPDATE jobs SET
                    job_description  = COALESCE(?, job_description),
                    education_level  = COALESCE(?, education_level),
                    experience_level = COALESCE(?, experience_level),
                    last_updated     = CURRENT_TIMESTAMP
                WHERE job_url = ?
            """, (description or None, education or None, experience or None, job_url))
            conn.commit()

    def count_live(self) -> int:
        with sqlite3.connect(self.db_path) as conn:
            return conn.execute(
                "SELECT COUNT(*) FROM jobs WHERE status = 'Live' AND is_valid = 1"
            ).fetchone()[0]

    def export_csv(self, csv_path: Path):
        import csv
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                "SELECT * FROM jobs WHERE is_valid = 1 AND status = 'Live'"
            )
            cols = [d[0] for d in cursor.description]
            rows = cursor.fetchall()
        with open(csv_path, "w", encoding="utf-8", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(cols)
            writer.writerows(rows)
        logger.info(f"CSV exporté : {csv_path} ({len(rows)} offres)")


# ─────────────────────────── Phase 1 : API ──────────────────────

def _api_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "Accept": "application/json",
        "User-Agent": "Mozilla/5.0 (Taleos-Scraper/2.0)",
    })
    return s


def fetch_all_jobs_from_api() -> List[Dict]:
    """
    Collecte toutes les offres JP Morgan via l'API Oracle HCM REST.
    Retourne une liste de dicts bruts (items[0].requisitionList).
    """
    session = _api_session()
    all_jobs: List[Dict] = []
    offset = 0
    total_known = None

    logger.info("Phase 1 — Collecte des offres via API Oracle HCM...")
    while True:
        url = (
            f"{BASE_API}"
            f"?finder=findReqs;siteNumber={SITE_NUMBER},limit={BATCH_SIZE},offset={offset}"
            f"&expand=requisitionList"
        )
        try:
            resp = session.get(url, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
            data = resp.json()
        except Exception as exc:
            logger.error(f"Erreur API offset={offset}: {exc}")
            break

        items = data.get("items", [])
        if not items:
            break

        meta    = items[0]
        batch   = meta.get("requisitionList", [])
        if total_known is None:
            total_known = meta.get("TotalJobsCount", 0)
            logger.info(f"  Total annoncé par l'API : {total_known} offres")

        all_jobs.extend(batch)
        logger.info(f"  Batch offset={offset} → {len(batch)} offres (total={len(all_jobs)})")

        if not batch or len(all_jobs) >= total_known:
            break
        offset += BATCH_SIZE
        time.sleep(0.3)  # politesse

    logger.info(f"Phase 1 terminée — {len(all_jobs)} offres collectées")
    return all_jobs


def transform_api_item(item: Dict) -> Dict:
    """Convertit un item brut de l'API en dict Taleos."""
    job_id_raw   = str(item.get("Id") or "")
    title        = (item.get("Title") or "").strip()
    posted_date  = item.get("PostedDate") or ""
    location_raw = (item.get("PrimaryLocation") or "").strip()
    iso_country  = (item.get("PrimaryLocationCountry") or "").strip()
    oracle_family = (item.get("JobFamily") or "").strip()
    short_desc   = (item.get("ShortDescriptionStr") or "").strip()
    workplace    = (item.get("WorkplaceType") or "").strip()
    contract_raw = (item.get("ContractType") or "").strip()
    job_type_raw = (item.get("JobType") or "").strip()

    job_url = DETAIL_URL.format(job_id=job_id_raw)
    job_id  = f"JPM_{job_id_raw}"

    # Contrat
    contract_type = contract_raw or job_type_raw or infer_contract_type(title)

    # Localisation
    location = parse_location(location_raw, iso_country)

    # Famille métier (Phase 2 complètera avec la description)
    job_family = map_job_family(oracle_family, title, short_desc)

    # Niveau d'expérience depuis le titre seul (Phase 2 affinera)
    experience_level = extract_experience_level(short_desc, contract_type, title)

    # Poste de management ? Senior / Head / Director / Managing Director / VP / MD / Chief
    is_management = bool(re.search(
        r'\b(head|director|managing director|MD|VP|vice.?president|chief|CDO|CTO|CIO|CFO|CEO)\b',
        title, re.I
    ))

    return {
        "job_url":            job_url,
        "job_id":             job_id,
        "job_title":          title,
        "contract_type":      contract_type,
        "publication_date":   posted_date,
        "location":           location,
        "job_family":         job_family,
        "duration":           "",
        "management_position": "Oui" if is_management else "Non",
        "status":             "Live",
        "education_level":    "",
        "experience_level":   experience_level or "",
        "training_specialization": "",
        "technical_skills":   "[]",
        "behavioral_skills":  "[]",
        "tools":              "",
        "languages":          "",
        "job_description":    short_desc,  # sera complété en Phase 2
        "company_name":       COMPANY_NAME,
        "company_description": "",
        "is_valid":           1,
    }


# ─────────────────────── Phase 2 : Playwright ───────────────────

async def _navigate(page, url: str, max_retries: int = 3):
    for attempt in range(max_retries):
        try:
            await page.goto(url, timeout=PAGE_TIMEOUT, wait_until="domcontentloaded")
            return
        except Exception as exc:
            if attempt == max_retries - 1:
                raise
            wait = 2 ** attempt
            logger.debug(f"Retry {attempt+1} ({exc}) → wait {wait}s")
            await asyncio.sleep(wait)


async def _scrape_detail(context: BrowserContext, job_url: str,
                          sem: asyncio.Semaphore) -> tuple[str, Optional[str], Optional[str], Optional[str]]:
    """
    Retourne (job_url, description, education_level, experience_level).
    Bloque les images/médias pour accélérer le chargement.
    """
    async with sem:
        page = await context.new_page()
        try:
            await page.route(
                "**/*",
                lambda route: route.abort()
                if route.request.resource_type in ("image", "media", "font", "stylesheet")
                else route.continue_(),
            )
            await _navigate(page, job_url)
            await asyncio.sleep(1.0)  # SPA : laisser le JS rendre le contenu

            html = await page.content()
            soup = BeautifulSoup(html, "html.parser")

            # Description : chercher le bloc principal du contenu
            desc_text = ""
            selectors = [
                ".job-description",
                ".requisition-detail",
                "[data-bind*='description']",
                "div.details",
                "article",
                "main",
            ]
            for sel in selectors:
                el = soup.select_one(sel)
                if el and len(el.get_text(strip=True)) > 100:
                    desc_text = el.get_text(separator=" ", strip=True)
                    break

            # Fallback : balise <body> entière (texte > 200 car.)
            if not desc_text:
                body = soup.find("body")
                if body:
                    desc_text = body.get_text(separator=" ", strip=True)

            # Tronquer à 25 000 caractères, supprimer le bruit post-offre
            for stopper in ["Similar Jobs", "Jobs you may be interested in", "Postuler"]:
                idx = desc_text.find(stopper)
                if idx > 200:
                    desc_text = desc_text[:idx]
            desc_text = re.sub(r"\s+", " ", desc_text).strip()[:25_000]

            education = extract_education(desc_text)
            experience = extract_experience_level(desc_text, job_title=None)

            return job_url, desc_text or None, education, experience

        except Exception as exc:
            logger.warning(f"Detail failed: {job_url} — {exc}")
            return job_url, None, None, None
        finally:
            await page.close()


async def scrape_descriptions(urls: List[str], db: Database):
    """Scrape les descriptions pour les URLs données (nouvelles offres uniquement)."""
    if not urls:
        logger.info("Phase 2 — Aucune nouvelle description à scraper")
        return
    if not PLAYWRIGHT_AVAILABLE:
        logger.warning("Phase 2 ignorée — playwright non installé")
        return

    logger.info(f"Phase 2 — Scraping des descriptions ({len(urls)} offres)...")
    sem = asyncio.Semaphore(CONCURRENCY)
    done = 0

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=Config.HEADLESS)
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                       "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )
        try:
            tasks = [_scrape_detail(context, url, sem) for url in urls]
            for coro in asyncio.as_completed(tasks):
                job_url, desc, edu, exp = await coro
                if desc:
                    db.upsert_description(job_url, desc, edu, exp)
                done += 1
                if done % 50 == 0:
                    logger.info(f"  Phase 2 : {done}/{len(urls)} descriptions scrappées")
        finally:
            await browser.close()

    logger.info(f"Phase 2 terminée — {done} descriptions traitées")


# ─────────────────────────── Main ───────────────────────────────

async def main():
    db = Database(Config.DB_PATH)
    t0 = time.time()

    # ── Phase 1 : API ──────────────────────────────────────────
    api_items = fetch_all_jobs_from_api()
    if not api_items:
        logger.error("Phase 1 a échoué — aucune offre collectée. Arrêt.")
        return

    api_jobs   = [transform_api_item(item) for item in api_items]
    api_urls   = {j["job_url"] for j in api_jobs}

    # Marquer les offres qui ne sont plus dans l'API comme expirées
    live_in_db = db.get_live_urls()
    newly_expired = live_in_db - api_urls
    if newly_expired:
        logger.info(f"  → {len(newly_expired)} offres disparues de l'API → Expired")
        db.mark_expired(newly_expired)

    # Upsert toutes les offres actives
    new_count = 0
    for job in api_jobs:
        is_new = job["job_url"] not in live_in_db
        if is_new:
            new_count += 1
        db.upsert(job)

    logger.info(f"Phase 1 — {len(api_jobs)} offres upsertées ({new_count} nouvelles)")

    # ── Phase 2 : descriptions (nouvelles offres seulement) ────
    urls_without_desc = list(db.get_urls_without_description())
    if urls_without_desc:
        await scrape_descriptions(urls_without_desc, db)

    # ── Stats finales ──────────────────────────────────────────
    total_live = db.count_live()
    elapsed    = time.time() - t0
    logger.info(f"\n{'='*60}")
    logger.info(f"✅ JP Morgan — {total_live} offres Live en base")
    logger.info(f"   Durée : {elapsed:.0f}s")
    logger.info(f"   Base  : {Config.DB_PATH}")
    logger.info(f"{'='*60}")

    # Export CSV optionnel
    db.export_csv(Config.CSV_PATH)


if __name__ == "__main__":
    asyncio.run(main())
