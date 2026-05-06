#!/usr/bin/env python3
"""
GOLDMAN SACHS — JOB SCRAPER
Extrait les offres d'emploi via l'API GraphQL interne de higher.gs.com.
100% requêtes HTTP — pas de Playwright nécessaire (descriptionHtml incluse dans la réponse).

Endpoint : https://api-higher.gs.com/gateway/api/v1/graphql
"""

import json
import logging
import re
import sqlite3
import time
import uuid
from datetime import datetime
from html.parser import HTMLParser
from pathlib import Path
from typing import Dict, List, Optional, Set

import requests

try:
    from country_normalizer import normalize_country
    from job_family_classifier import classify_job_family
    from experience_extractor import extract_experience_level
except ImportError:
    import sys
    sys.path.append(str(Path(__file__).parent))
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
GQL_ENDPOINT  = "https://api-higher.gs.com/gateway/api/v1/graphql"
DETAIL_URL    = "https://higher.gs.com/roles/{source_id}"
COMPANY_NAME  = "Goldman Sachs"

PAGE_SIZE     = 100
REQUEST_DELAY = 0.5   # secondes entre les pages API
REQUEST_TIMEOUT = 30
MAX_RETRIES   = 3

# Expériences à inclure (INTERNAL_MOBILITY exclus = postes internes GS)
# CAMPUS inclus : programmes undergraduate + internships d'été (périmètre Taleos)
EXPERIENCES = ["PROFESSIONAL", "EARLY_CAREER", "CAMPUS"]

# ─────────────────────────── Config ─────────────────────────────
class Config:
    BASE_DIR  = Path(__file__).parent
    DB_PATH   = BASE_DIR / "goldman_sachs_jobs.db"
    CSV_PATH  = BASE_DIR / "goldman_sachs_jobs.csv"

# ─────────────── GraphQL query ──────────────────────────────────
GQL_QUERY = """
query GetRoles($input: RoleSearchQueryInput!) {
  roleSearch(searchQueryInput: $input) {
    totalCount
    items {
      roleId
      jobTitle
      corporateTitle
      division
      jobFunction
      skillset
      educationLevel
      locations {
        primary
        city
        state
        country
      }
      jobType {
        code
        description
      }
      lastPostedDate
      status
      shortDescription
      descriptionHtml
      externalSource {
        sourceId
      }
    }
  }
}
"""

# ─────────────── Division → Taleos job family ───────────────────
DIVISION_TO_FAMILY: Dict[str, str] = {
    # Technologie
    "Technology Division":              "IT, Digital et Data",
    "Engineering Division":             "IT, Digital et Data",
    "Platform Solutions":               "IT, Digital et Data",
    # Finance / Marchés
    "Global Markets Division":          "Financement et Investissement",
    "Investment Banking Division":      "Financement et Investissement",
    "Global Banking & Markets":         "Financement et Investissement",
    "Investment Banking":               "Financement et Investissement",
    "Consumer & Wealth Management":     "Commercial / Relations Clients",
    "Private Wealth Management":        "Commercial / Relations Clients",
    # Asset & Wealth Management est une division large → classifié par jobFunction
    # (voir map_job_family qui fallback sur classify_job_family pour cette division)
    # Risque / Conformité
    "Risk Division":                    "Risques / Contrôles permanents",
    "Compliance Division":              "Conformité / Sécurité financière",
    "Internal Audit Division":          "Audit / Inspection",
    "Legal Division":                   "Juridique",
    # Finance d'entreprise
    "Finance Division":                 "Finances / Comptabilité / Contrôle de gestion",
    "Controllers":                      "Finances / Comptabilité / Contrôle de gestion",
    "Treasury":                         "Finances / Comptabilité / Contrôle de gestion",
    # Opérations
    "Operations Division":              "Gestion des opérations",
    # RH / Support
    "Human Capital Management":         "RH / Formation / Communication",
    "Corporate Planning & Management":  "Organisation / Projet / PMO",
    "Corporate Communications":         "RH / Formation / Communication",
    "Global Investment Research":       "Analyse financière et économique",
    "Research":                         "Analyse financière et économique",
    # Autres
    "Realty Management Division":       "Gestion des opérations",
    "GS Bank":                          "Commercial / Relations Clients",
    "Marcus by Goldman Sachs":          "Commercial / Relations Clients",
    "Transaction Banking":              "Gestion des opérations",
    "Ayco":                             "Commercial / Relations Clients",
}

# ─────────────── CorporateTitle → experience level ──────────────
TITLE_TO_EXPERIENCE: Dict[str, str] = {
    "summer analyst":             "0 - 2 ans",
    "intern":                     "0 - 2 ans",
    "analyst":                    "0 - 2 ans",
    "summer associate":           "3 - 5 ans",
    "associate":                  "3 - 5 ans",
    "vice president":             "6 - 10 ans",
    "executive director":         "11 ans et plus",
    "extended managing director": "11 ans et plus",
    "managing director":          "11 ans et plus",
    "partner":                    "11 ans et plus",
    "support":                    "0 - 2 ans",   # rôles de support / admin
}

# ─────────────── Contract type inference ────────────────────────
_CONTRACT_PATTERNS = [
    (re.compile(r'\bsummer\s+(analyst|associate|intern)\b', re.I), "Stage"),
    (re.compile(r'\bintern\b|\binternship\b|\bstagiaire\b', re.I),  "Stage"),
    (re.compile(r'\bcampus\b|\bgraduate\s+scheme\b', re.I),          "Stage"),
    (re.compile(r'\bapprentice\b|\bapprentissage\b|\balternance\b', re.I), "Alternance / Apprentissage"),
    (re.compile(r'\bfixed[\s-]term\b|\btemporar\w+\b', re.I),       "CDD"),
]

def infer_contract_type(role_id: str, title: str, corporate_title: str) -> str:
    """Infère le type de contrat. CDI par défaut pour Goldman Sachs."""
    # roleId suffix indique l'expérience type
    if "_GS_CAMPUS" in role_id.upper():
        return "Stage"
    ct_lower = (corporate_title or "").lower()
    if ct_lower == "intern":
        return "Stage"
    for pattern, contract in _CONTRACT_PATTERNS:
        if pattern.search(title):
            return contract
    return "CDI"


# ─────────────── Location normalisation ─────────────────────────
def build_location(locations: List[Dict]) -> str:
    """
    Prend la liste de locations GS et retourne "Ville - Pays (FR)".
    Préfère la location primaire.
    """
    if not locations:
        return ""
    primary = next((l for l in locations if l.get("primary")), locations[0])
    city    = (primary.get("city") or "").strip()
    state   = (primary.get("state") or "").strip()
    country_raw = (primary.get("country") or "").strip()
    country_fr  = normalize_country(country_raw) if country_raw else ""

    # Certaines offres GS n'ont pas `city` mais stockent la ville dans `state`
    # (ex: Lima / Peru). On s'appuie donc sur `state` en fallback.
    if not city and state:
        city = state
    elif city and state and city.lower() == country_raw.lower() and state.lower() != country_raw.lower():
        city = state

    # Corrections de noms de villes
    CITY_CORRECTIONS = {
        "new york": "New York",
        "london": "Londres",
        "hong kong": "Hong-Kong",
        "singapore": "Singapour",
        "tokyo": "Tokyo",
        "frankfurt": "Francfort",
        "frankfurt am main": "Francfort",
        "zurich": "Zurich",
        "zürich": "Zurich",
        "warsaw": "Varsovie",
        "dubai": "Dubaï",
        "ho chi minh city": "Ho Chi Minh City",
        "seoul": "Seoul",
        "riyadh": "Riyadh",
        "nassau": "Nassau",
        "panama city": "Panama City",
        "kuwait city": "Kuwait City",
    }
    city_fr = CITY_CORRECTIONS.get(city.lower(), city)

    if city_fr and country_fr:
        # Éviter redondance (ex: Hong-Kong - Hong-Kong)
        if city_fr.lower().replace("-", " ") == country_fr.lower().replace("-", " "):
            return country_fr
        return f"{city_fr} - {country_fr}"
    return country_fr or city_fr or country_raw


# ─────────────── HTML → plain text ──────────────────────────────
class _HTMLStripper(HTMLParser):
    def __init__(self):
        super().__init__()
        self._parts: List[str] = []

    def handle_data(self, data):
        self._parts.append(data)

    def get_text(self) -> str:
        return re.sub(r'\s+', ' ', ' '.join(self._parts)).strip()


def html_to_text(html: str) -> str:
    if not html:
        return ""
    parser = _HTMLStripper()
    try:
        parser.feed(html)
        return parser.get_text()[:25_000]
    except Exception:
        return re.sub(r'<[^>]+>', ' ', html)[:25_000]


# ─────────────── Education extraction ───────────────────────────
def extract_education(api_value: Optional[str], description: str) -> Optional[str]:
    """Extrait le niveau d'éducation depuis le champ API (souvent null) ou la description."""
    if api_value:
        val = api_value.lower().strip()
        for pattern, level in [
            (r'ph\.?d|doctorat|doctorate',                   "Bac + 8 / Doctorat"),
            (r'master|mba|m\.?s\b|bac\s*\+\s*5|grande\s+école|engineering\s+school', "Bac + 5 / M2 et plus"),
            (r'bachelor|bac\s*\+\s*3|licence',               "Bac + 3 / L3"),
            (r'bac\s*\+\s*2|bts|dut',                        "Bac + 2 / L2"),
        ]:
            if re.search(pattern, val, re.I):
                return level

    text = (description or "").lower()
    for pattern, level in [
        (r'ph\.?d\.?|doctorat|doctorate',                              "Bac + 8 / Doctorat"),
        (r'master|mba|m\.?s\b|bac\s*\+\s*5|grande\s+école|'
         r'engineering\s+school|business\s+school',                    "Bac + 5 / M2 et plus"),
        (r'bachelor|bac\s*\+\s*3|licence|undergraduate',               "Bac + 3 / L3"),
        (r'bac\s*\+\s*2|bts|dut',                                      "Bac + 2 / L2"),
    ]:
        if re.search(pattern, text):
            return level
    return None


# ─────────────── Job family mapping ─────────────────────────────
def map_job_family(division: str, title: str, description: str,
                   job_function: str = "") -> str:
    """
    Mappe la division GS vers la taxonomie Taleos.
    Pour les divisions larges (Asset & Wealth Management, Global Banking & Markets),
    on utilise le jobFunction et le titre pour affiner.
    """
    # Divisions larges qui nécessitent un affinement par jobFunction/title
    BROAD_DIVISIONS = {
        "asset & wealth management",
        "global banking & markets",
        "consumer & wealth management",
    }

    if division:
        # Exact match en priorité
        for key, family in DIVISION_TO_FAMILY.items():
            if key.lower() == division.lower():
                # Pour les divisions larges : affiner via jobFunction + title
                if division.lower() in BROAD_DIVISIONS:
                    refined = _refine_by_function(job_function, title, description)
                    if refined:
                        return refined
                return family

        # Matching partiel
        div_lower = division.lower()
        if "technolog" in div_lower or "engineering" in div_lower:
            return "IT, Digital et Data"
        if "risk" in div_lower:
            return "Risques / Contrôles permanents"
        if "compliance" in div_lower:
            return "Conformité / Sécurité financière"
        if "human capital" in div_lower:
            return "RH / Formation / Communication"
        if "legal" in div_lower:
            return "Juridique"
        if "finance" in div_lower or "controller" in div_lower:
            return "Finances / Comptabilité / Contrôle de gestion"
        if "operation" in div_lower:
            return "Gestion des opérations"
        if "audit" in div_lower:
            return "Audit / Inspection"
        if "research" in div_lower:
            return "Analyse financière et économique"
        if "market" in div_lower or "banking" in div_lower or "trading" in div_lower:
            return "Financement et Investissement"
        if "wealth" in div_lower or "asset" in div_lower:
            # Division large sans exact match → classify_job_family
            refined = _refine_by_function(job_function, title, description)
            return refined or "Financement et Investissement"

    return classify_job_family(title, description or "")


def _refine_by_function(job_function: str, title: str, description: str) -> Optional[str]:
    """Affine la famille via le jobFunction quand la division est trop large."""
    combined = f"{job_function} {title}".lower()
    if re.search(r'\bsoftware|engineer|data|technology|digital|cloud|platform|'
                 r'architect|devops|cyber|infrastructure|machine\s+learning|AI\b', combined):
        return "IT, Digital et Data"
    if re.search(r'\bcomplaince|compliance|regulatory|aml|kyc\b', combined):
        return "Conformité / Sécurité financière"
    if re.search(r'\brisk\b', combined):
        return "Risques / Contrôles permanents"
    if re.search(r'\baudit\b', combined):
        return "Audit / Inspection"
    if re.search(r'\boperations?|ops\b|transaction|settlement|clearing\b', combined):
        return "Gestion des opérations"
    if re.search(r'\bproject\s+manag|program\s+manag|pmo\b|portfolio\s+manag', combined):
        return "Organisation / Projet / PMO"
    if re.search(r'\bmarketing|communications?\b', combined):
        return "RH / Formation / Communication"
    if re.search(r'\baccount\s*manag|sales|advisor|wealth\s+manag|client\s+serv|'
                 r'relationship\s+manag|private\s+bank\b', combined):
        return "Commercial / Relations Clients"
    if re.search(r'\bquant|research|analyst|investing|portfolio|trading|markets?\b', combined):
        return "Financement et Investissement"
    if re.search(r'\bfinance|accounting|controller|reporting\b', combined):
        return "Finances / Comptabilité / Contrôle de gestion"
    if re.search(r'\bhuman\s+capital|hr\b|recruit|talent', combined):
        return "RH / Formation / Communication"
    if re.search(r'\blegal|counsel\b', combined):
        return "Juridique"
    return None


# ─────────────── Experience level from corporateTitle ───────────
def map_experience_level(corporate_title: str, contract_type: str,
                          description: str, title: str) -> Optional[str]:
    """Mappe le niveau d'expérience depuis le titre corporate GS."""
    # Stage / Alternance → toujours 0-2 ans
    if contract_type in ("Stage", "Alternance / Apprentissage", "VIE"):
        return "0 - 2 ans"

    ct_lower = (corporate_title or "").lower().strip()
    for key, level in TITLE_TO_EXPERIENCE.items():
        if key in ct_lower:
            return level

    # Fallback : extraction depuis la description
    return extract_experience_level(description, contract_type, title)


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

    def get_existing_date(self, job_url: str) -> Optional[str]:
        with sqlite3.connect(self.db_path) as conn:
            row = conn.execute(
                "SELECT publication_date, first_seen FROM jobs WHERE job_url = ?",
                (job_url,)
            ).fetchone()
            if not row:
                return None
            return row[0] or (str(row[1])[:10] if row[1] else None)

    def upsert(self, job: Dict):
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
                    job_title        = excluded.job_title,
                    contract_type    = excluded.contract_type,
                    publication_date = COALESCE(excluded.publication_date, jobs.publication_date),
                    location         = excluded.location,
                    job_family       = excluded.job_family,
                    status           = excluded.status,
                    education_level  = COALESCE(excluded.education_level, jobs.education_level),
                    experience_level = COALESCE(excluded.experience_level, jobs.experience_level),
                    job_description  = COALESCE(excluded.job_description, jobs.job_description),
                    management_position = excluded.management_position,
                    company_name     = excluded.company_name,
                    last_updated     = CURRENT_TIMESTAMP,
                    is_valid         = excluded.is_valid
            """, {
                "job_url":                url,
                "job_id":                 job.get("job_id", ""),
                "job_title":              job.get("job_title", ""),
                "contract_type":          job.get("contract_type", ""),
                "publication_date":       job.get("publication_date", ""),
                "location":               job.get("location", ""),
                "job_family":             job.get("job_family", ""),
                "duration":               "",
                "management_position":    job.get("management_position", ""),
                "status":                 "Live",
                "education_level":        job.get("education_level", ""),
                "experience_level":       job.get("experience_level", ""),
                "training_specialization": "",
                "technical_skills":       "[]",
                "behavioral_skills":      "[]",
                "tools":                  "",
                "languages":              "",
                "job_description":        job.get("job_description", ""),
                "company_name":           COMPANY_NAME,
                "company_description":    job.get("company_description", ""),
                "is_valid":               1,
            })
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
            csv.writer(f).writerow(cols)
            csv.writer(f).writerows(rows)
        logger.info(f"CSV exporté : {csv_path} ({len(rows)} offres)")


# ─────────────────────── API layer ──────────────────────────────

def _make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "Content-Type": "application/json",
        "Origin": "https://higher.gs.com",
        "Referer": "https://higher.gs.com/results",
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
        ),
    })
    return s


def _gql_request(session: requests.Session, variables: Dict,
                 attempt: int = 1) -> Optional[Dict]:
    """Exécute une requête GraphQL avec retry exponentiel."""
    session.headers["x-higher-request-id"] = str(uuid.uuid4())
    try:
        resp = session.post(
            GQL_ENDPOINT,
            json={"query": GQL_QUERY, "variables": variables},
            timeout=REQUEST_TIMEOUT,
        )
        resp.raise_for_status()
        data = resp.json()
        if "errors" in data and data.get("data") is None:
            raise ValueError(f"GraphQL error: {data['errors'][0]['message']}")
        return data.get("data", {}).get("roleSearch")
    except Exception as exc:
        if attempt < MAX_RETRIES:
            wait = 2 ** attempt
            logger.warning(f"  Retry {attempt}/{MAX_RETRIES} après {wait}s ({exc})")
            time.sleep(wait)
            return _gql_request(session, variables, attempt + 1)
        logger.error(f"  Échec définitif: {exc}")
        return None


def fetch_all_jobs() -> List[Dict]:
    """Collecte toutes les offres GS via l'API GraphQL (pagination 100/page)."""
    session = _make_session()
    all_items: List[Dict] = []
    page_number = 1
    total_known: Optional[int] = None

    logger.info("Collecte des offres Goldman Sachs via GraphQL...")

    while True:
        variables = {
            "input": {
                "page": {"pageSize": PAGE_SIZE, "pageNumber": page_number},
                "sort": {"sortStrategy": "POSTED_DATE", "sortOrder": "DESC"},
                "experiences": EXPERIENCES,
                "filters": [],
            }
        }

        result = _gql_request(session, variables)
        if result is None:
            logger.error(f"Erreur page {page_number} — arrêt")
            break

        items = result.get("items", [])
        if total_known is None:
            total_known = result.get("totalCount", 0)
            logger.info(f"  Total annoncé : {total_known} offres")

        all_items.extend(items)
        logger.info(f"  Page {page_number} → {len(items)} offres (total={len(all_items)})")

        if not items or len(all_items) >= total_known:
            break

        page_number += 1
        time.sleep(REQUEST_DELAY)

    logger.info(f"Collecte terminée — {len(all_items)} offres")
    return all_items


# ─────────────── Item → Taleos job dict ─────────────────────────

def transform_item(item: Dict) -> Dict:
    """Convertit un item GraphQL en dict Taleos."""
    role_id       = item.get("roleId", "")
    source_id     = (item.get("externalSource") or {}).get("sourceId", "")
    title         = (item.get("jobTitle") or "").strip()
    corp_title    = (item.get("corporateTitle") or "").strip()
    division      = (item.get("division") or "").strip()
    job_function  = (item.get("jobFunction") or "").strip()
    skillset      = (item.get("skillset") or "").strip()
    education_api = item.get("educationLevel")
    locations     = item.get("locations") or []
    job_type_obj  = item.get("jobType") or {}
    posted_raw    = item.get("lastPostedDate") or ""
    short_desc    = (item.get("shortDescription") or "").strip()
    desc_html     = item.get("descriptionHtml") or ""

    # URL et ID
    job_url = DETAIL_URL.format(source_id=source_id) if source_id else ""
    job_id  = f"GS_{source_id}" if source_id else f"GS_{role_id}"

    # Date de publication (ISO → YYYY-MM-DD)
    pub_date = ""
    if posted_raw:
        pub_date = posted_raw[:10]  # "2026-04-29T20:18:42.211Z" → "2026-04-29"

    # Description texte (HTML strippé)
    desc_text = html_to_text(desc_html) or short_desc

    # Contrat
    contract_type = infer_contract_type(role_id, title, corp_title)

    # Localisation
    location = build_location(locations)

    # Famille de métier
    job_family = map_job_family(division, title, desc_text, job_function)

    # Education
    education_level = extract_education(education_api, desc_text)

    # Expérience
    experience_level = map_experience_level(corp_title, contract_type, desc_text, title)

    # Poste de management (VP+)
    is_management = bool(re.search(
        r'\b(vice\s*president|managing\s*director|executive\s*director|partner|'
        r'head|chief|MD|VP)\b',
        f"{title} {corp_title}", re.I
    ))

    # Company description (division + fonction)
    company_desc_parts = []
    if division:
        company_desc_parts.append(f"Division: {division}")
    if job_function:
        company_desc_parts.append(f"Fonction: {job_function}")
    if corp_title:
        company_desc_parts.append(f"Niveau: {corp_title}")
    if skillset:
        company_desc_parts.append(f"Expertise: {skillset}")

    return {
        "job_url":          job_url,
        "job_id":           job_id,
        "job_title":        title,
        "contract_type":    contract_type,
        "publication_date": pub_date,
        "location":         location,
        "job_family":       job_family,
        "management_position": "Oui" if is_management else "Non",
        "education_level":  education_level or "",
        "experience_level": experience_level or "",
        "job_description":  desc_text,
        "company_name":     COMPANY_NAME,
        "company_description": " | ".join(company_desc_parts),
        "is_valid":         1,
    }


# ─────────────────────────── Main ───────────────────────────────

def main():
    db = Database(Config.DB_PATH)
    t0 = time.time()

    # Collecter toutes les offres
    api_items = fetch_all_jobs()
    if not api_items:
        logger.error("Aucune offre collectée — arrêt.")
        return

    api_jobs   = [transform_item(item) for item in api_items]
    api_urls   = {j["job_url"] for j in api_jobs if j["job_url"]}

    # Marquer les offres disparues comme expirées
    live_in_db = db.get_live_urls()
    newly_expired = live_in_db - api_urls
    if newly_expired:
        logger.info(f"  → {len(newly_expired)} offres disparues de l'API → Expired")
        db.mark_expired(newly_expired)

    # Upsert
    new_count = sum(1 for j in api_jobs if j["job_url"] not in live_in_db)
    for job in api_jobs:
        db.upsert(job)

    total_live = db.count_live()
    elapsed    = time.time() - t0

    logger.info(f"\n{'='*60}")
    logger.info(f"✅ Goldman Sachs — {total_live} offres Live en base")
    logger.info(f"   Nouvelles : {new_count} | Expirées : {len(newly_expired)}")
    logger.info(f"   Durée     : {elapsed:.0f}s")
    logger.info(f"   Base      : {Config.DB_PATH}")
    logger.info(f"{'='*60}")

    # Stats de distribution
    with sqlite3.connect(Config.DB_PATH) as conn:
        print("\n=== Top 10 localisations ===")
        for loc, n in conn.execute(
            "SELECT location, COUNT(*) n FROM jobs WHERE status='Live' GROUP BY location ORDER BY n DESC LIMIT 10"
        ).fetchall():
            print(f"  {n:4} | {loc}")

        print("\n=== Types de contrat ===")
        for ct, n in conn.execute(
            "SELECT contract_type, COUNT(*) n FROM jobs WHERE status='Live' GROUP BY contract_type ORDER BY n DESC"
        ).fetchall():
            print(f"  {n:4} | {ct}")

        print("\n=== Familles de métier ===")
        for jf, n in conn.execute(
            "SELECT job_family, COUNT(*) n FROM jobs WHERE status='Live' GROUP BY job_family ORDER BY n DESC"
        ).fetchall():
            print(f"  {n:4} | {jf}")

        print("\n=== Niveaux d'expérience ===")
        for el, n in conn.execute(
            "SELECT experience_level, COUNT(*) n FROM jobs WHERE status='Live' GROUP BY experience_level ORDER BY n DESC"
        ).fetchall():
            print(f"  {n:4} | {el}")

    db.export_csv(Config.CSV_PATH)


if __name__ == "__main__":
    main()
