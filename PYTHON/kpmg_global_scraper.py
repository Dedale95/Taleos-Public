#!/usr/bin/env python3
"""
KPMG Global — Job Scraper
=========================
Marchés couverts (principaux marchés européens, nord-américains et Asie-Pacifique) :

  Radancy/TalentBrew (HTML)     : France, Allemagne, Italie
  SmartRecruiters (API JSON)    : Australie
  Lever (API JSON)              : Nouvelle-Zélande
  Drupal/LunrJS (API JSON)      : Pays-Bas
  HR-Manager (API JSON)         : Danemark
  WordPress HTML                : États-Unis

Delta scraping :
  - Pages listing TOUJOURS fetchées (détection des expiries)
  - Pages détail uniquement pour les nouvelles URLs absentes de la DB

Base commune : kpmg_jobs.db (company_name = "KPMG {Pays}" par marché)
"""

import json
import logging
import re
import sqlite3
import sys
import time
import unicodedata
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

import requests
from bs4 import BeautifulSoup

try:
    from job_family_classifier import classify_job_family
    from experience_extractor import extract_experience_level
    from country_normalizer import normalize_country
except ImportError:
    sys.path.append(str(Path(__file__).parent))
    from job_family_classifier import classify_job_family
    from experience_extractor import extract_experience_level
    from country_normalizer import normalize_country

# ─────────────────────────── Logging ────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)

# ─────────────────────────── Constantes globales ────────────────────────────
DB_PATH         = Path(__file__).parent / "kpmg_jobs.db"
REQUEST_DELAY   = 1.2    # entre pages listing
DETAIL_DELAY    = 0.9    # entre pages détail
REQUEST_TIMEOUT = 30
MAX_RETRIES     = 3

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9,fr;q=0.8",
}

JSON_HEADERS = {**HEADERS, "Accept": "application/json, */*"}

# ─────────────────────────── Sources ────────────────────────────────────────
RADANCY_SOURCES = [
    {
        "company_name":  "KPMG France",
        "base_url":      "https://emplois.kpmg.fr",
        "search_path":   "/recherche-d%27offres",
        "page1_params":  "?k=&l=",   # paramètres page 1 (sans p=)
        "country":       "France",
        "country_code":  "FR",
    },
    {
        "company_name":  "KPMG Allemagne",
        "base_url":      "https://jobs.kpmg.de",
        "search_path":   "/search/",
        "page1_params":  "",
        "country":       "Allemagne",
        "country_code":  "DE",
    },
    {
        "company_name":  "KPMG Italie",
        "base_url":      "https://careers.kpmg.it",
        "search_path":   "/search/",
        "page1_params":  "",
        "country":       "Italie",
        "country_code":  "IT",
    },
]

SMARTRECRUITERS_SOURCES = [
    {
        "company_name": "KPMG Australie",
        "company_id":   "KPMGAustralia1",
        "country":      "Australie",
    },
]

LEVER_SOURCES = [
    {
        "company_name": "KPMG Nouvelle-Zélande",
        "company_slug": "kpmgnz",
        "country":      "Nouvelle-Zélande",
    },
]

LUNR_SOURCES = [
    {
        "company_name": "KPMG Pays-Bas",
        "base_url":     "https://werkenbijkpmg.nl",
        "api_path":     "/_lunr/vacancies",
        "country":      "Pays-Bas",
    },
]

HRMANAGER_SOURCES = [
    {
        "company_name": "KPMG Danemark",
        "customer":     "kpmg",
        "country":      "Danemark",
    },
]

WORDPRESS_SOURCES = [
    {
        "company_name": "KPMG États-Unis",
        "base_url":     "https://kpmguscareers.com",
        "search_path":  "/job-search/",
        "country":      "États-Unis",
    },
]


# ─────────────────────────── HTTP helpers ───────────────────────────────────
def fetch_text(url: str, session: requests.Session,
               headers: Optional[Dict] = None) -> Optional[str]:
    h = headers or HEADERS
    for attempt in range(MAX_RETRIES):
        try:
            r = session.get(url, headers=h, timeout=REQUEST_TIMEOUT)
            r.raise_for_status()
            return r.text
        except Exception as e:
            if attempt < MAX_RETRIES - 1:
                time.sleep(2 ** attempt)
            else:
                logger.warning(f"fetch_text échec {url}: {e}")
    return None


def fetch_json(url: str, session: requests.Session,
               params: Optional[Dict] = None):
    for attempt in range(MAX_RETRIES):
        try:
            r = session.get(url, headers=JSON_HEADERS, params=params,
                            timeout=REQUEST_TIMEOUT)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            if attempt < MAX_RETRIES - 1:
                time.sleep(2 ** attempt)
            else:
                logger.warning(f"fetch_json échec {url}: {e}")
    return None


# ─────────────────────────── Normalisation commune ───────────────────────────
def _norm(s: str) -> str:
    s = unicodedata.normalize("NFKD", s or "")
    return "".join(c for c in s if not unicodedata.combining(c)).lower()


_CONTRACT_MAP: Dict[str, str] = {
    # Français
    "cdi":          "CDI",
    "cdd":          "CDD",
    "stage":        "Stage",
    "alternance":   "Alternance",
    "alternant":    "Alternance",
    "libéral":      "Indépendant / Entrepreneur",
    "freelance":    "Indépendant / Entrepreneur",
    "vie":          "V.I.E.",
    "v.i.e":        "V.I.E.",
    "intérim":      "Intérim",
    "interim":      "Intérim",
    # Anglais
    "permanent":    "CDI",
    "full-time":    "CDI",
    "full time":    "CDI",
    "casual":       "CDI",   # Australie : emploi permanent sans horaires garantis
    "graduate programme": "CDD",  # Programme diplômant à durée déterminée (12-24 mois)
    "graduate program": "CDD",
    "trainee":      "Stage",
    "fixed term":   "CDD",
    "contract":     "CDD",
    "temporary":    "Intérim",
    "temp ":        "Intérim",
    "internship":   "Stage",
    "intern":       "Stage",
    "placement":    "Stage",
    "apprentice":   "Alternance",
    "part-time":    "Temps partiel",
    "part time":    "Temps partiel",
    # Allemand
    "unbefristet":  "CDI",
    "befristet":    "CDD",
    "praktikum":    "Stage",
    "ausbildung":   "Alternance",
    "teilzeit":     "Temps partiel",
    # Italien
    "indeterminato": "CDI",
    "determinato":   "CDD",
    "tirocinio":     "Stage",
}

def parse_contract(raw: str) -> str:
    if not raw:
        return ""
    low = raw.strip().lower()
    for k, v in _CONTRACT_MAP.items():
        if k in low:
            return v
    return raw.strip()


_EDUCATION_MAP: Dict[str, str] = {
    "bac +4/5":  "Bac+5/Master 2",
    "bac+4/5":   "Bac+5/Master 2",
    "bac +5":    "Bac+5/Master 2",
    "bac+5":     "Bac+5/Master 2",
    "master's":  "Bac+5/Master 2",
    "masters":   "Bac+5/Master 2",
    "master":    "Bac+5/Master 2",
    "bac +4":    "Bac+4/Master 1",
    "bac+4":     "Bac+4/Master 1",
    "bac +3":    "Bac+3/Licence",
    "bac+3":     "Bac+3/Licence",
    "bachelor":  "Bac+3/Licence",
    "bac +2/3":  "Bac+2",
    "bac+2/3":   "Bac+2",
    "bac +2":    "Bac+2",
    "bac+2":     "Bac+2",
    "bac +2/3":  "Bac+2",
    "doctorat":  "Doctorat/PhD",
    "doctorate": "Doctorat/PhD",
    "phd":       "Doctorat/PhD",
}

def parse_education(raw: str) -> str:
    if not raw:
        return ""
    low = raw.strip().lower()
    for k, v in _EDUCATION_MAP.items():
        if k in low:
            return v
    return ""


_NL_EXP_MAP: Dict[str, str] = {
    "starter":   "0 - 2 ans",
    "junior":    "0 - 2 ans",
    "student":   "0 - 2 ans",
    "stagiair":  "0 - 2 ans",
    "graduate":  "0 - 2 ans",
    "medior":    "3 - 5 ans",
    "senior":    "6 - 10 ans",
    "lead":      "6 - 10 ans",
    "manager":   "6 - 10 ans",
    "director":  "11 ans et plus",
    "partner":   "11 ans et plus",
}

def nl_map_experience(raw: str) -> str:
    low = (raw or "").lower()
    for k, v in _NL_EXP_MAP.items():
        if k in low:
            return v
    return ""


_SR_EXP_MAP: Dict[str, str] = {
    "entry level":       "0 - 2 ans",
    "entry-level":       "0 - 2 ans",
    "mid-senior level":  "3 - 5 ans",
    "mid senior level":  "3 - 5 ans",
    "mid level":         "3 - 5 ans",
    "senior":            "6 - 10 ans",
    "director":          "11 ans et plus",
    "executive":         "11 ans et plus",
    "internship":        "0 - 2 ans",
}

def sr_map_experience(label: str) -> str:
    low = (label or "").lower()
    for k, v in _SR_EXP_MAP.items():
        if k in low:
            return v
    return ""

def sr_map_contract(label: str) -> str:
    low = (label or "").lower()
    if "intern" in low:       return "Stage"
    if "part" in low:         return "Temps partiel"
    if "temporary" in low:    return "Intérim"
    if "fixed" in low:        return "CDD"
    if "contract" in low:     return "CDD"
    if "full" in low:         return "CDI"
    if "permanent" in low:    return "CDI"
    return parse_contract(label)


def parse_date_ms(ms: int) -> str:
    """Timestamp ms → YYYY-MM-DD."""
    try:
        dt = datetime.fromtimestamp(ms / 1000, tz=timezone.utc)
        return dt.strftime("%Y-%m-%d")
    except Exception:
        return datetime.now().strftime("%Y-%m-%d")


def parse_hrmanager_date(date_str: str) -> str:
    """/Date(1234567890000+0200)/ → YYYY-MM-DD."""
    m = re.search(r"/Date\((\d+)", date_str or "")
    if m:
        return parse_date_ms(int(m.group(1)))
    return datetime.now().strftime("%Y-%m-%d")


def apply_nlp(job: Dict) -> Dict:
    """Applique job_family + experience_level via NLP si vides."""
    title   = job.get("job_title", "")
    desc    = job.get("job_description", "")
    contract = job.get("contract_type", "")

    if not job.get("job_family") and (title or desc):
        fam = classify_job_family(title, desc)
        if fam:
            job["job_family"] = fam

    if not job.get("experience_level"):
        if contract in ("Stage", "Alternance"):
            job["experience_level"] = "0 - 2 ans"
        elif title or desc:
            exp = extract_experience_level(title, desc, contract)
            if exp:
                job["experience_level"] = exp

    return job


# ─────────────────────────── Base de données ────────────────────────────────
class Database:
    def __init__(self, db_path: Path = DB_PATH):
        self.db_path = db_path
        self._init()

    def _init(self):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS jobs (
                    job_url                  TEXT PRIMARY KEY,
                    job_id                   TEXT,
                    job_title                TEXT,
                    contract_type            TEXT,
                    publication_date         TEXT,
                    location                 TEXT,
                    job_family               TEXT,
                    duration                 TEXT,
                    management_position      INTEGER DEFAULT 0,
                    status                   TEXT DEFAULT 'Live',
                    company_name             TEXT,
                    job_description          TEXT,
                    company_description      TEXT,
                    experience_level         TEXT,
                    education_level          TEXT,
                    training_specialization  TEXT,
                    technical_skills         TEXT,
                    behavioral_skills        TEXT,
                    tools                    TEXT,
                    languages                TEXT,
                    country                  TEXT,
                    region                   TEXT,
                    source                   TEXT DEFAULT 'KPMG',
                    first_seen               TEXT,
                    last_updated             TEXT,
                    is_valid                 INTEGER DEFAULT 1
                )
            """)
            conn.commit()

        # Migrations : ajouter les colonnes manquantes dans les DBs existantes
        _EXTRA_COLS = [
            ("duration",                "TEXT"),
            ("management_position",     "INTEGER DEFAULT 0"),
            ("company_description",     "TEXT"),
            ("training_specialization", "TEXT"),
            ("technical_skills",        "TEXT"),
            ("behavioral_skills",       "TEXT"),
            ("tools",                   "TEXT"),
            ("languages",               "TEXT"),
            ("country",                 "TEXT"),
            ("region",                  "TEXT"),
        ]
        with sqlite3.connect(self.db_path) as conn:
            for col, col_type in _EXTRA_COLS:
                try:
                    conn.execute(f"ALTER TABLE jobs ADD COLUMN {col} {col_type}")
                except sqlite3.OperationalError:
                    pass  # colonne déjà présente
            conn.commit()

    def get_live_urls_for(self, company_name: str) -> Set[str]:
        with sqlite3.connect(self.db_path) as conn:
            rows = conn.execute(
                "SELECT job_url FROM jobs WHERE company_name=? AND status='Live' AND is_valid=1",
                (company_name,),
            ).fetchall()
        return {r[0] for r in rows}

    def get_all_urls(self) -> Set[str]:
        with sqlite3.connect(self.db_path) as conn:
            rows = conn.execute("SELECT job_url FROM jobs").fetchall()
        return {r[0] for r in rows}

    def expire_missing(self, company_name: str, current_urls: Set[str]):
        live = self.get_live_urls_for(company_name)
        to_expire = live - current_urls
        if not to_expire:
            return
        ph = ",".join("?" * len(to_expire))
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                f"UPDATE jobs SET status='Expired', last_updated=CURRENT_TIMESTAMP "
                f"WHERE job_url IN ({ph})",
                list(to_expire),
            )
            conn.commit()
        logger.info(f"  ⚰️  {len(to_expire)} offre(s) expirée(s) [{company_name}]")

    def upsert(self, job: Dict):
        url = job.get("job_url", "")
        if not url:
            return
        j = {k: v for k, v in job.items() if not k.startswith("_")}
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                INSERT INTO jobs (
                    job_url, job_id, job_title, contract_type, publication_date,
                    location, job_family, status, company_name, job_description,
                    experience_level, education_level, country, region, source,
                    first_seen, last_updated, is_valid
                ) VALUES (
                    :job_url, :job_id, :job_title, :contract_type, :publication_date,
                    :location, :job_family, 'Live', :company_name, :job_description,
                    :experience_level, :education_level, :country, :region, 'KPMG',
                    CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, 1
                )
                ON CONFLICT(job_url) DO UPDATE SET
                    job_title        = excluded.job_title,
                    contract_type    = CASE WHEN excluded.contract_type != '' THEN excluded.contract_type ELSE jobs.contract_type END,
                    location         = excluded.location,
                    job_family       = excluded.job_family,
                    status           = 'Live',
                    company_name     = excluded.company_name,
                    job_description  = COALESCE(excluded.job_description, jobs.job_description),
                    experience_level = excluded.experience_level,
                    education_level  = excluded.education_level,
                    country          = excluded.country,
                    region           = excluded.region,
                    last_updated     = CURRENT_TIMESTAMP,
                    is_valid         = 1
            """, {
                "job_url":          url,
                "job_id":           j.get("job_id", ""),
                "job_title":        j.get("job_title", ""),
                "contract_type":    j.get("contract_type", ""),
                "publication_date": j.get("publication_date", ""),
                "location":         j.get("location", ""),
                "job_family":       j.get("job_family", ""),
                "company_name":     j.get("company_name", "KPMG"),
                "job_description":  j.get("job_description", ""),
                "experience_level": j.get("experience_level", ""),
                "education_level":  j.get("education_level", ""),
                "country":          j.get("country", ""),
                "region":           j.get("region", ""),
            })
            conn.commit()


# ═══════════════════════════════════════════════════════════════════════════════
#  HANDLER 1 : Radancy / TalentBrew  (France · Allemagne · Italie)
# ═══════════════════════════════════════════════════════════════════════════════
RADANCY_JOBS_PER_PAGE = 15


def _playwright_available() -> bool:
    """Vérifie si Playwright est installé dans l'environnement."""
    try:
        from playwright.sync_api import sync_playwright  # noqa: F401
        return True
    except ImportError:
        return False


def fetch_html_playwright(url: str) -> Optional[str]:
    """Récupère le HTML d'une page via Playwright (navigateur headless).
    Contourne la protection anti-bot des IPs datacenter (ex. GitHub Actions).
    Retourne None si Playwright n'est pas disponible ou en cas d'erreur.
    """
    try:
        from playwright.sync_api import sync_playwright
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            ctx = browser.new_context(
                user_agent=(
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/124.0.0.0 Safari/537.36"
                ),
                locale="fr-FR",
                viewport={"width": 1280, "height": 900},
            )
            page = ctx.new_page()
            page.goto(url, wait_until="domcontentloaded", timeout=30_000)
            # Attendre que les cartes offres soient chargées (data-job-id ou /job/ links)
            try:
                page.wait_for_selector("[data-job-id], a[href*='/job/']", timeout=8_000)
            except Exception:
                pass  # on prend quand même le HTML courant
            content = page.content()
            browser.close()
            return content
    except Exception as e:
        logger.warning(f"  Playwright indisponible ou erreur : {e}")
        return None


def radancy_page_url(source: Dict, page: int) -> str:
    base         = source["base_url"]
    search_path  = source["search_path"]
    page1_params = source.get("page1_params", "")

    if page == 1:
        return f"{base}{search_path}{page1_params}"

    if "?" in page1_params:
        return f"{base}{search_path}{page1_params}&p={page}"
    else:
        return f"{base}{search_path}{page1_params}?p={page}"


def radancy_parse_listing(html: str, source: Dict) -> List[Dict]:
    soup         = BeautifulSoup(html, "html.parser")
    base_url     = source["base_url"]
    country      = source["country"]
    country_code = source.get("country_code", country[:2].upper())
    company      = source["company_name"]
    jobs         = []

    # Stratégie 1 : cartes avec data-job-id (format Radancy standard — France, Allemagne)
    cards_with_id = soup.find_all(attrs={"data-job-id": True})

    # Stratégie 2 : liens /job/ID/ (format Radancy alternatif — Italie)
    # Chaque lien /job/slug/ID/ est le titre de l'offre.
    if not cards_with_id:
        seen_hrefs: Set[str] = set()
        for a in soup.find_all("a", href=re.compile(r"/job/.+/\d+/?$")):
            href = a.get("href", "").strip()
            if not href or href in seen_hrefs:
                continue
            seen_hrefs.add(href)
            if not href.startswith("http"):
                href = base_url + href
            m = re.search(r"/(\d+)/?$", href)
            job_id_raw = m.group(1) if m else href.rstrip("/").split("/")[-1]
            title = a.get_text(strip=True)
            if not title:
                # Chercher le titre dans le parent proche
                parent = a.parent
                for _ in range(3):
                    if parent and parent.get_text(strip=True):
                        title = parent.get_text(strip=True)[:120]
                        break
                    parent = parent.parent if parent else None
            if not title:
                continue
            jobs.append({
                "job_id":           f"KPMG_{country_code}_{job_id_raw}",
                "job_url":          href,
                "job_title":        title,
                "location":         country,
                "region":           "",
                "country":          country,
                "contract_type":    "",
                "job_family":       "",
                "experience_level": "",
                "company_name":     company,
                "publication_date": "",
                "job_description":  "",
                "education_level":  "",
                "_category":        "",
                "_speciality":      "",
            })
        return jobs

    for card in cards_with_id:
        job_id_raw = str(card.get("data-job-id", "")).strip()
        if not job_id_raw:
            continue

        # Priorité : href sur la carte elle-même (France — <a data-job-id="..." href="...">)
        # Fallback : lien enfant (ancien format Radancy)
        href = str(card.get("href") or "").strip()
        if not href:
            link = card.find("a", href=True)
            href = str(link.get("href", "")).strip() if link else ""
        if href and not href.startswith("http"):
            href = base_url + href
        if not href:
            continue

        # Titre : double underscore BEM (job-list__title) ou tiret (job-list-title)
        title_el = card.find(class_=re.compile(r"\bjob[-_]list[-_]{1,2}title\b", re.I))
        if not title_el:
            title_el = card.find(["h2", "h3"])
        title = title_el.get_text(strip=True) if title_el else ""
        if not title:
            continue

        loc_el   = card.find(class_=re.compile(r"\bjob[-_]list[-_]{1,2}location\b", re.I))
        loc_raw  = loc_el.get_text(strip=True) if loc_el else ""
        parts    = [p.strip() for p in loc_raw.split(",")]
        city     = parts[0] if parts else ""
        region   = parts[1] if len(parts) > 1 else ""
        location = f"{city} - {country}" if city else country

        ct_el    = card.find(class_=re.compile(r"\bjob[-_]list[-_]{1,2}contract\b", re.I))
        ct_raw   = ct_el.get_text(strip=True) if ct_el else ""

        cat_el   = card.find(class_=re.compile(r"\bjob[-_]list[-_]{1,2}category\b", re.I))
        category = cat_el.get_text(strip=True) if cat_el else ""

        spec_el    = card.find(class_=re.compile(r"\bjob[-_]list[-_]{1,2}speciality\b", re.I))
        speciality = spec_el.get_text(strip=True) if spec_el else ""

        contract  = parse_contract(ct_raw)
        exp_level = "0 - 2 ans" if contract in ("Stage", "Alternance") else ""

        jobs.append({
            "job_id":           f"KPMG_{country_code}_{job_id_raw}",
            "job_url":          href,
            "job_title":        title,
            "location":         location,
            "region":           region,
            "country":          country,
            "contract_type":    contract,
            "job_family":       "",
            "experience_level": exp_level,
            "company_name":     company,
            "publication_date": "",
            "job_description":  "",
            "education_level":  "",
            "_category":        category,
            "_speciality":      speciality,
        })

    return jobs


def radancy_get_total_pages(html: str) -> int:
    soup     = BeautifulSoup(html, "html.parser")
    max_page = 1

    for a in soup.find_all("a", href=True):
        m = re.search(r"[?&]p=(\d+)", a["href"])
        if m:
            max_page = max(max_page, int(m.group(1)))

    for el in soup.find_all(
        string=re.compile(
            r"\d+\s*(offre|emploi|résultat|ergebnis|annuncio|risultato|vacature|job)",
            re.I,
        )
    ):
        m = re.search(r"(\d+)", el)
        if m:
            total = int(m.group(1))
            if 10 < total < 5000:
                max_page = max(
                    max_page,
                    (total + RADANCY_JOBS_PER_PAGE - 1) // RADANCY_JOBS_PER_PAGE,
                )
                break

    return max_page


def radancy_enrich_detail(html: str, job: Dict) -> Dict:
    """Enrichit un job depuis sa page détail Radancy (JSON-LD + sidebar)."""
    soup    = BeautifulSoup(html, "html.parser")
    j       = {k: v for k, v in job.items() if not k.startswith("_")}
    country = j.get("country", "")

    # ── JSON-LD JobPosting ───────────────────────────────────────────────────
    for script in soup.find_all("script", type="application/ld+json"):
        try:
            raw  = (script.string or "").strip()
            data = json.loads(raw)
            if isinstance(data, list):
                data = next((d for d in data if d.get("@type") == "JobPosting"), None) or {}
            if data.get("@type") != "JobPosting":
                continue

            if not j.get("publication_date") and data.get("datePosted"):
                dp = str(data["datePosted"])
                try:
                    parts = [p.zfill(2) for p in dp.split("-")]
                    j["publication_date"] = "-".join(parts[:3])
                except Exception:
                    j["publication_date"] = dp[:10]

            desc_raw = data.get("description", "")
            if desc_raw:
                j["job_description"] = BeautifulSoup(
                    desc_raw, "html.parser"
                ).get_text(separator="\n", strip=True)

            locations = data.get("jobLocation", [])
            if isinstance(locations, dict):
                locations = [locations]
            if locations:
                addr   = locations[0].get("address", {})
                city   = addr.get("addressLocality", "")
                region = addr.get("addressRegion", "")
                c      = addr.get("addressCountry", country)
                c_norm = normalize_country(c) or c
                if city:
                    j["location"] = f"{city} - {c_norm}"
                    if region and not j.get("region"):
                        j["region"] = region
                elif c_norm:
                    j["location"] = c_norm
                j["country"] = c_norm

            if not j.get("contract_type") and data.get("employmentType"):
                j["contract_type"] = parse_contract(data["employmentType"])

            break
        except Exception:
            continue

    # ── Sidebar (champs complémentaires) ────────────────────────────────────
    for cls, handler in [
        (r"\bjob-contract-type\b", lambda v: {"contract_type": parse_contract(v)}),
        (r"\bjob-education\b",     lambda v: {"education_level": parse_education(v)}),
    ]:
        el = soup.find(class_=re.compile(cls, re.I))
        if el:
            val = el.get_text(strip=True)
            if val:
                for k, v in handler(val).items():
                    if v:
                        j[k] = v

    # ── NLP fallback ─────────────────────────────────────────────────────────
    j = apply_nlp(j)
    j.setdefault("publication_date", datetime.now().strftime("%Y-%m-%d"))
    return j


def scrape_radancy(source: Dict, db: Database, session: requests.Session):
    company = source["company_name"]
    logger.info(f"\n{'='*58}")
    logger.info(f"  {company}  [{source['base_url']}]")
    logger.info(f"{'='*58}")

    all_jobs: List[Dict] = []
    seen_urls: Set[str]  = set()

    first_url  = radancy_page_url(source, 1)
    first_html = fetch_text(first_url, session)
    if not first_html:
        logger.warning(f"  ❌ Page 1 inaccessible — marché ignoré")
        return

    total_pages = radancy_get_total_pages(first_html)
    logger.info(f"  Pages estimées : {total_pages}")

    # Vérifier si la page 1 contient des cartes offres.
    # Les IPs datacenter (GitHub Actions) reçoivent une version sans cartes (protection anti-bot).
    # Si 0 cartes en requests → basculer sur Playwright pour récupérer le HTML rendu.
    use_playwright = False
    probe_jobs = radancy_parse_listing(first_html, source)
    if not probe_jobs and _playwright_available():
        logger.info(f"  ⚠️ Page 1 : 0 offres avec requests → tentative Playwright (IP datacenter détectée)")
        pw_html = fetch_html_playwright(first_url)
        if pw_html:
            probe_jobs_pw = radancy_parse_listing(pw_html, source)
            if probe_jobs_pw:
                logger.info(f"  ✅ Playwright : {len(probe_jobs_pw)} offres sur page 1 → mode Playwright activé")
                first_html = pw_html
                use_playwright = True
                total_pages = radancy_get_total_pages(first_html) or total_pages
            else:
                logger.warning(f"  ❌ Playwright également 0 offres — marché peut-être vide ou hors ligne")
        else:
            logger.warning(f"  ❌ Playwright indisponible ou échec — abandon fallback")

    for page in range(1, total_pages + 5):
        if page == 1:
            html = first_html
        elif use_playwright:
            html = fetch_html_playwright(radancy_page_url(source, page))
        else:
            html = fetch_text(radancy_page_url(source, page), session)
        if not html:
            logger.warning(f"  Page {page} : échec fetch — arrêt")
            break

        jobs = radancy_parse_listing(html, source)
        if not jobs:
            logger.info(f"  Page {page} : 0 offres — arrêt")
            break

        new = [j for j in jobs if j["job_url"] not in seen_urls]
        seen_urls.update(j["job_url"] for j in jobs)
        all_jobs.extend(new)
        logger.info(f"  Page {page}/{total_pages} : {len(jobs)} ({len(new)} nouvelles)")

        if page > 1:
            time.sleep(REQUEST_DELAY)

    logger.info(f"  Total listing : {len(all_jobs)} offres")
    db.expire_missing(company, {j["job_url"] for j in all_jobs})

    existing_urls = db.get_all_urls()
    to_enrich = [j for j in all_jobs if j["job_url"] not in existing_urls]
    to_update  = [j for j in all_jobs if j["job_url"] in existing_urls]
    logger.info(f"  🔍 {len(to_enrich)} nouvelles · {len(to_update)} mises à jour")

    # contract_types actuels en DB pour les offres existantes
    existing_contracts_radancy: Dict[str, str] = {}
    if to_update:
        with sqlite3.connect(db.db_path) as _conn:
            _urls = [j["job_url"] for j in to_update]
            ph = ",".join("?" * len(_urls))
            for row in _conn.execute(f"SELECT job_url, contract_type FROM jobs WHERE job_url IN ({ph})", _urls):
                existing_contracts_radancy[row[0]] = row[1] or ""

    for job in to_update:
        job.pop("_category", None)
        job.pop("_speciality", None)
        # Si contract_type vide en DB → ré-enrichir depuis la page détail
        if not job.get("contract_type") and not existing_contracts_radancy.get(job["job_url"]):
            html = fetch_text(job["job_url"], session)
            if html:
                enriched = radancy_enrich_detail(html, job)
                job = enriched
            if not job.get("contract_type"):
                job["contract_type"] = "CDI"
            time.sleep(DETAIL_DELAY)
        apply_nlp(job)
        db.upsert(job)

    for i, job in enumerate(to_enrich, 1):
        logger.info(f"    [{i}/{len(to_enrich)}] {job['job_title'][:65]}")
        html = fetch_text(job["job_url"], session)
        enriched = radancy_enrich_detail(html, job) if html else {
            k: v for k, v in job.items() if not k.startswith("_")
        }
        if not html:
            apply_nlp(enriched)
        db.upsert(enriched)
        time.sleep(DETAIL_DELAY)

    logger.info(f"  ✅ {company} : {len(to_enrich)} nouvelles · {len(to_update)} mises à jour")


# ═══════════════════════════════════════════════════════════════════════════════
#  HANDLER 2 : SmartRecruiters API  (Australie)
# ═══════════════════════════════════════════════════════════════════════════════
_SR_API   = "https://api.smartrecruiters.com/v1/companies/{company_id}/postings"
_SR_LIMIT = 100


def scrape_smartrecruiters(source: Dict, db: Database, session: requests.Session):
    company    = source["company_name"]
    company_id = source["company_id"]
    country    = source["country"]
    api_url    = _SR_API.format(company_id=company_id)

    logger.info(f"\n{'='*58}")
    logger.info(f"  {company}  [SmartRecruiters → {company_id}]")
    logger.info(f"{'='*58}")

    all_jobs: List[Dict] = []
    offset = 0

    while True:
        data = fetch_json(api_url, session, params={"limit": _SR_LIMIT, "offset": offset})
        if not data or not isinstance(data, dict):
            break

        items = data.get("content", []) or []
        total = data.get("totalFound", 0)
        if not items:
            break

        logger.info(f"  offset={offset} · {len(items)} offres (total={total})")

        for item in items:
            job_id      = str(item.get("id", "")).strip()
            title       = item.get("name", "").strip()
            # L'API SmartRecruiters de KPMG Australie n'expose pas postingUrl/applyUrl.
            # On construit l'URL publique depuis l'identifiant company + job id.
            posting_url = (
                item.get("postingUrl")
                or item.get("applyUrl")
                or (f"https://jobs.smartrecruiters.com/{company_id}/{job_id}" if job_id else "")
            )
            if not title or not posting_url:
                continue

            loc    = item.get("location") or {}
            city   = loc.get("city", "") or ""
            region = loc.get("region", "") or ""
            c_raw  = loc.get("country", country) or country
            c_norm = normalize_country(c_raw) or c_raw
            location = f"{city} - {c_norm}" if city else c_norm

            dept      = (item.get("department") or {})
            dept_label = dept.get("label", "")

            exp_raw      = ((item.get("experienceLevel") or {}).get("label") or "")
            contract_raw = ((item.get("typeOfEmployment") or {}).get("label") or "")

            released = item.get("releasedDate", "") or ""
            pub_date = released[:10] if len(released) >= 10 else datetime.now().strftime("%Y-%m-%d")

            contract  = sr_map_contract(contract_raw)
            exp_level = sr_map_experience(exp_raw)
            if not exp_level and contract == "Stage":
                exp_level = "0 - 2 ans"

            all_jobs.append({
                "job_id":           f"SR_{job_id}",
                "job_url":          posting_url,
                "job_title":        title,
                "location":         location,
                "region":           region,
                "country":          c_norm,
                "contract_type":    contract,
                "job_family":       "",
                "experience_level": exp_level,
                "company_name":     company,
                "publication_date": pub_date,
                "job_description":  "",
                "education_level":  "",
                "_dept":            dept_label,
            })

        offset += _SR_LIMIT
        if offset >= total:
            break
        time.sleep(REQUEST_DELAY)

    logger.info(f"  Total listing : {len(all_jobs)} offres")
    db.expire_missing(company, {j["job_url"] for j in all_jobs})

    existing_urls = db.get_all_urls()
    to_enrich = [j for j in all_jobs if j["job_url"] not in existing_urls]
    to_update  = [j for j in all_jobs if j["job_url"] in existing_urls]
    logger.info(f"  🔍 {len(to_enrich)} nouvelles · {len(to_update)} mises à jour")

    for job in to_enrich + to_update:
        dept = job.pop("_dept", "")
        if not job.get("job_family"):
            fam = classify_job_family(job["job_title"], dept)
            if fam:
                job["job_family"] = fam
        apply_nlp(job)
        db.upsert(job)

    logger.info(f"  ✅ {company} : {len(to_enrich)} nouvelles · {len(to_update)} mises à jour")


# ═══════════════════════════════════════════════════════════════════════════════
#  HANDLER 3 : Lever API  (Nouvelle-Zélande)
# ═══════════════════════════════════════════════════════════════════════════════
_LEVER_API = "https://api.lever.co/v0/postings/{slug}?mode=json"


def scrape_lever(source: Dict, db: Database, session: requests.Session):
    company = source["company_name"]
    slug    = source["company_slug"]
    country = source["country"]
    url     = _LEVER_API.format(slug=slug)

    logger.info(f"\n{'='*58}")
    logger.info(f"  {company}  [Lever → {slug}]")
    logger.info(f"{'='*58}")

    data = fetch_json(url, session)
    if not data or not isinstance(data, list):
        logger.warning(f"  ❌ Aucune donnée Lever pour {slug}")
        return

    all_jobs: List[Dict] = []

    for item in data:
        job_id     = item.get("id", "")
        title      = (item.get("text") or "").strip()
        hosted_url = item.get("hostedUrl") or item.get("applyUrl") or ""
        if not title or not hosted_url:
            continue

        cats    = item.get("categories") or {}
        loc_raw = (cats.get("location") or "").strip()
        team    = (cats.get("team") or "").strip()
        commit  = (cats.get("commitment") or "").strip()

        location = f"{loc_raw} - {country}" if loc_raw else country

        created_ms = item.get("createdAt", 0) or 0
        pub_date   = parse_date_ms(created_ms) if created_ms else datetime.now().strftime("%Y-%m-%d")

        contract  = parse_contract(commit) or "CDI"  # Lever/NZ : commitment vide → CDI permanent
        exp_level = "0 - 2 ans" if contract == "Stage" else ""

        desc_plain = (item.get("descriptionPlain") or "").strip()
        if not desc_plain:
            desc_raw = (item.get("description") or "").strip()
            if desc_raw:
                desc_plain = BeautifulSoup(desc_raw, "html.parser").get_text(
                    separator="\n", strip=True
                )

        all_jobs.append({
            "job_id":           f"LVR_{job_id}",
            "job_url":          hosted_url,
            "job_title":        title,
            "location":         location,
            "region":           "",
            "country":          country,
            "contract_type":    contract,
            "job_family":       "",
            "experience_level": exp_level,
            "company_name":     company,
            "publication_date": pub_date,
            "job_description":  desc_plain,
            "education_level":  "",
            "_team":            team,
        })

    logger.info(f"  Total listing : {len(all_jobs)} offres")
    db.expire_missing(company, {j["job_url"] for j in all_jobs})

    existing_urls = db.get_all_urls()
    to_enrich = [j for j in all_jobs if j["job_url"] not in existing_urls]
    to_update  = [j for j in all_jobs if j["job_url"] in existing_urls]
    logger.info(f"  🔍 {len(to_enrich)} nouvelles · {len(to_update)} mises à jour")

    for job in to_enrich + to_update:
        team = job.pop("_team", "")
        if not job.get("job_family"):
            fam = classify_job_family(job["job_title"], f"{job.get('job_description','')} {team}")
            if fam:
                job["job_family"] = fam
        apply_nlp(job)
        db.upsert(job)

    logger.info(f"  ✅ {company} : {len(to_enrich)} nouvelles · {len(to_update)} mises à jour")


# ═══════════════════════════════════════════════════════════════════════════════
#  HANDLER 4 : Drupal / LunrJS  (Pays-Bas)
# ═══════════════════════════════════════════════════════════════════════════════
def scrape_lunr(source: Dict, db: Database, session: requests.Session):
    company  = source["company_name"]
    base_url = source["base_url"]
    api_path = source["api_path"]
    country  = source["country"]
    url      = f"{base_url}{api_path}"

    logger.info(f"\n{'='*58}")
    logger.info(f"  {company}  [LunrJS → {url}]")
    logger.info(f"{'='*58}")

    data = fetch_json(url, session)
    if not data or not isinstance(data, list):
        logger.warning(f"  ❌ Aucune donnée LunrJS pour {base_url}")
        return

    all_jobs: List[Dict] = []

    for item in data:
        title = (item.get("title") or "").strip()
        if not title:
            continue

        # URL extraite du HTML de la card
        card_html = (item.get("card") or "")
        job_url   = ""
        desc      = ""
        if card_html:
            card_soup = BeautifulSoup(card_html, "html.parser")
            a_tag = card_soup.find("a", href=True)
            if a_tag:
                href = a_tag["href"].strip()
                job_url = href if href.startswith("http") else base_url + href
            # Description sommaire depuis la card (exclure le titre)
            for h in card_soup.find_all(["h1", "h2", "h3", "h4"]):
                h.decompose()
            desc = card_soup.get_text(separator="\n", strip=True)

        if not job_url:
            continue

        # Localisation
        locs    = item.get("term_location") or []
        loc_raw = locs[0] if isinstance(locs, list) and locs else (locs if isinstance(locs, str) else "")
        location = f"{loc_raw} - {country}" if loc_raw else country

        # Expérience
        exps    = item.get("term_experience") or []
        exp_raw = exps[0] if isinstance(exps, list) and exps else (exps if isinstance(exps, str) else "")
        exp_level = nl_map_experience(exp_raw)

        # Éducation
        edus    = item.get("term_education") or []
        edu_raw = edus[0] if isinstance(edus, list) and edus else (edus if isinstance(edus, str) else "")

        # Marchés (pour NLP famille)
        markets     = item.get("term_markets") or []
        markets_str = ", ".join(markets) if isinstance(markets, list) else str(markets)

        # Date
        created_raw = (item.get("created") or "")
        pub_date    = datetime.now().strftime("%Y-%m-%d")
        if created_raw:
            try:
                pub_date = datetime.fromisoformat(created_raw[:10]).strftime("%Y-%m-%d")
            except Exception:
                pass

        job_id = job_url.rstrip("/").split("/")[-1] or str(abs(hash(job_url)))

        all_jobs.append({
            "job_id":           f"NL_{job_id}",
            "job_url":          job_url,
            "job_title":        title,
            "location":         location,
            "region":           "",
            "country":          country,
            "contract_type":    "",
            "job_family":       "",
            "experience_level": exp_level,
            "company_name":     company,
            "publication_date": pub_date,
            "job_description":  desc,
            "education_level":  parse_education(edu_raw),
            "_markets":         markets_str,
            "_exp_raw":         exp_raw,
        })

    logger.info(f"  Total listing : {len(all_jobs)} offres")
    db.expire_missing(company, {j["job_url"] for j in all_jobs})

    existing_urls = db.get_all_urls()
    to_enrich = [j for j in all_jobs if j["job_url"] not in existing_urls]
    to_update  = [j for j in all_jobs if j["job_url"] in existing_urls]
    logger.info(f"  🔍 {len(to_enrich)} nouvelles · {len(to_update)} mises à jour")

    for job in to_enrich + to_update:
        markets = job.pop("_markets", "")
        job.pop("_exp_raw", None)
        if not job.get("job_family"):
            fam = classify_job_family(job["job_title"], f"{job.get('job_description','')} {markets}")
            if fam:
                job["job_family"] = fam
        apply_nlp(job)
        db.upsert(job)

    logger.info(f"  ✅ {company} : {len(to_enrich)} nouvelles · {len(to_update)} mises à jour")


# ═══════════════════════════════════════════════════════════════════════════════
#  HANDLER 5 : HR-Manager JSON API  (Danemark)
# ═══════════════════════════════════════════════════════════════════════════════
_HRM_API = "https://api.hr-manager.net/JobPortal.svc/{customer}/PositionList/json/"


def scrape_hrmanager(source: Dict, db: Database, session: requests.Session):
    company  = source["company_name"]
    customer = source["customer"]
    country  = source["country"]
    url      = _HRM_API.format(customer=customer)

    logger.info(f"\n{'='*58}")
    logger.info(f"  {company}  [HR-Manager → {customer}]")
    logger.info(f"{'='*58}")

    data = fetch_json(url, session, params={"protype": "RecruitmentProject", "incads": "true"})
    if not data or not isinstance(data, dict):
        logger.warning(f"  ❌ Aucune donnée HR-Manager pour {customer}")
        return

    items    = data.get("Items") or []
    all_jobs: List[Dict] = []

    for item in items:
        title   = (item.get("Name") or "").strip()
        job_url = item.get("AdvertisementUrlSecure") or item.get("AdvertisementUrl") or ""
        if not title or not job_url:
            continue

        dept   = item.get("Department") or {}
        city   = (dept.get("City") or "").strip()
        c_raw  = (dept.get("Country") or country).strip()
        c_norm = normalize_country(c_raw) or c_raw
        location = f"{city} - {c_norm}" if city else c_norm

        cat    = ((item.get("PositionCategory") or {}).get("Name") or "")
        # PositionCategory.Name est le service (ex: "Management Consulting", "Deal Advisory")
        # pas le type de contrat. On utilise EmploymentType si présent, sinon CDI par défaut.
        emp_type_raw = ((item.get("EmploymentType") or {}).get("Name") or "")
        created   = parse_hrmanager_date(item.get("Created") or "")
        published = parse_hrmanager_date(item.get("Published") or "")
        pub_date  = published or created or datetime.now().strftime("%Y-%m-%d")

        desc = ""
        for advert in (item.get("Advertisements") or []):
            html_content = (advert.get("Content") or "").strip()
            if html_content:
                desc = BeautifulSoup(html_content, "html.parser").get_text(
                    separator="\n", strip=True
                )
                break

        job_id   = job_url.rstrip("/").split("/")[-1] or str(abs(hash(job_url)))
        contract = parse_contract(emp_type_raw) if emp_type_raw else parse_contract(cat)
        # Sécurité : si le résultat n'est pas un type connu (ex: département passé en cat),
        # on défaut à CDI — tous les postes KPMG DK sont des CDI permanents.
        _KNOWN_CT = {"CDI","CDD","Stage","Alternance","V.I.E.","Intérim","Temps partiel","Indépendant / Entrepreneur"}
        if contract not in _KNOWN_CT:
            contract = "CDI"

        all_jobs.append({
            "job_id":           f"DK_{job_id}",
            "job_url":          job_url,
            "job_title":        title,
            "location":         location,
            "region":           "",
            "country":          c_norm,
            "contract_type":    contract,
            "job_family":       "",
            "experience_level": "0 - 2 ans" if contract in ("Stage", "Alternance") else "",
            "company_name":     company,
            "publication_date": pub_date,
            "job_description":  desc,
            "education_level":  "",
            "_category":        cat,
        })

    logger.info(f"  Total listing : {len(all_jobs)} offres")
    db.expire_missing(company, {j["job_url"] for j in all_jobs})

    existing_urls = db.get_all_urls()
    to_enrich = [j for j in all_jobs if j["job_url"] not in existing_urls]
    to_update  = [j for j in all_jobs if j["job_url"] in existing_urls]
    logger.info(f"  🔍 {len(to_enrich)} nouvelles · {len(to_update)} mises à jour")

    for job in to_enrich + to_update:
        cat = job.pop("_category", "")
        if not job.get("job_family"):
            fam = classify_job_family(job["job_title"], f"{job.get('job_description','')} {cat}")
            if fam:
                job["job_family"] = fam
        apply_nlp(job)
        db.upsert(job)

    logger.info(f"  ✅ {company} : {len(to_enrich)} nouvelles · {len(to_update)} mises à jour")


# ═══════════════════════════════════════════════════════════════════════════════
#  HANDLER 6 : WordPress HTML  (États-Unis)
# ═══════════════════════════════════════════════════════════════════════════════
def wp_parse_listing(html: str, base_url: str, country: str, company: str) -> List[Dict]:
    soup  = BeautifulSoup(html, "html.parser")
    jobs  = []

    # Sélecteur confirmé lors de l'exploration : <a class="box-shadow d-block" data-id="...">
    cards = soup.find_all(
        "a",
        attrs={"data-id": True},
        class_=lambda c: c and "box-shadow" in c,
    )
    if not cards:
        # Fallback : chercher tout lien avec data-id
        cards = soup.find_all("a", attrs={"data-id": True, "href": True})

    for card in cards:
        href       = (card.get("href") or "").strip()
        job_id_raw = str(card.get("data-id", "")).strip()
        if not href:
            continue
        if not href.startswith("http"):
            href = base_url + href

        # Titre : h2/h3 en priorité, puis premier texte substantiel
        title_el = card.find(re.compile(r"^h[2-4]$", re.I))
        if not title_el:
            title_el = card.find(
                class_=re.compile(r"job[_-]?title|position[_-]?title|role[_-]?title", re.I)
            )
        title = title_el.get_text(strip=True) if title_el else ""
        if not title:
            all_parts = [t.strip() for t in card.get_text(separator="|").split("|") if t.strip()]
            title = all_parts[0] if all_parts else ""
        if not title:
            continue

        # Localisation
        loc_el  = card.find(class_=re.compile(r"location|city|address", re.I))
        loc_raw = loc_el.get_text(strip=True) if loc_el else ""
        location = f"{loc_raw} - {country}" if loc_raw else country

        # Type de contrat
        ct_el   = card.find(class_=re.compile(r"contract|employ|type|category", re.I))
        ct_raw  = ct_el.get_text(strip=True) if ct_el else ""
        contract = parse_contract(ct_raw)

        job_id = f"US_{job_id_raw}" if job_id_raw else f"US_{href.rstrip('/').split('/')[-1]}"

        jobs.append({
            "job_id":           job_id,
            "job_url":          href,
            "job_title":        title,
            "location":         location,
            "region":           "",
            "country":          country,
            "contract_type":    contract,
            "job_family":       "",
            "experience_level": "",
            "company_name":     company,
            "publication_date": datetime.now().strftime("%Y-%m-%d"),
            "job_description":  "",
            "education_level":  "",
        })

    return jobs


def wp_enrich_detail(html: str, job: Dict) -> Dict:
    """Enrichit depuis la page détail WordPress (JSON-LD + div description)."""
    soup = BeautifulSoup(html, "html.parser")
    j    = dict(job)

    # JSON-LD
    for script in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(script.string or "")
            if isinstance(data, list):
                data = next((d for d in data if d.get("@type") == "JobPosting"), None) or {}
            if data.get("@type") != "JobPosting":
                continue

            if data.get("datePosted"):
                dp = str(data["datePosted"])
                try:
                    parts = [p.zfill(2) for p in dp.split("-")]
                    j["publication_date"] = "-".join(parts[:3])
                except Exception:
                    j["publication_date"] = dp[:10]

            if not j.get("job_description") and data.get("description"):
                desc_raw = data["description"]
                j["job_description"] = BeautifulSoup(
                    desc_raw, "html.parser"
                ).get_text(separator="\n", strip=True)

            if not j.get("contract_type") and data.get("employmentType"):
                j["contract_type"] = parse_contract(data["employmentType"])

            # Mettre à jour location depuis JSON-LD si on a une ville (même si location déjà défini)
            if data.get("jobLocation"):
                locations = data["jobLocation"]
                if isinstance(locations, dict):
                    locations = [locations]
                if locations:
                    addr  = locations[0].get("address", {})
                    city  = addr.get("addressLocality", "").strip()
                    c     = addr.get("addressCountry", j.get("country", ""))
                    c_n   = normalize_country(c) or c
                    current_loc = j.get("location", "")
                    # Mettre à jour si on a une ville et que la location actuelle est pays-seulement
                    if city and (not current_loc or " - " not in current_loc):
                        j["location"] = f"{city} - {c_n}"
            break
        except Exception:
            continue

    # Description HTML si pas encore trouvée
    if not j.get("job_description"):
        desc_el = soup.find(
            class_=re.compile(r"job[_-]?description|job[_-]?content|entry[_-]?content|description", re.I)
        )
        if desc_el:
            j["job_description"] = desc_el.get_text(separator="\n", strip=True)

    return j


def wp_has_next_page(html: str) -> bool:
    soup = BeautifulSoup(html, "html.parser")
    if soup.find("a", rel="next"):
        return True
    if soup.find("a", class_=re.compile(r"\bnext\b", re.I)):
        return True
    # Certains thèmes utilisent un lien de pagination numéroté
    nav = soup.find(class_=re.compile(r"pagination|nav-links", re.I))
    if nav and nav.find("a", class_=re.compile(r"\bnext\b", re.I)):
        return True
    return False


def scrape_wordpress(source: Dict, db: Database, session: requests.Session):
    company     = source["company_name"]
    base_url    = source["base_url"]
    search_path = source["search_path"]
    country     = source["country"]

    logger.info(f"\n{'='*58}")
    logger.info(f"  {company}  [{base_url}]")
    logger.info(f"{'='*58}")

    all_jobs: List[Dict] = []
    seen_urls: Set[str]  = set()

    for page in range(1, 300):  # borne de sécurité
        url  = f"{base_url}{search_path}" if page == 1 else f"{base_url}{search_path}?paged={page}"
        html = fetch_text(url, session)
        if not html:
            logger.warning(f"  Page {page} : échec fetch — arrêt")
            break

        jobs = wp_parse_listing(html, base_url, country, company)
        if not jobs:
            logger.info(f"  Page {page} : 0 offres — arrêt")
            break

        new = [j for j in jobs if j["job_url"] not in seen_urls]
        seen_urls.update(j["job_url"] for j in jobs)
        all_jobs.extend(new)
        logger.info(f"  Page {page} : {len(jobs)} ({len(new)} nouvelles)")

        if not wp_has_next_page(html):
            logger.info(f"  Pas de page suivante — arrêt")
            break

        time.sleep(REQUEST_DELAY)

    logger.info(f"  Total listing : {len(all_jobs)} offres")
    db.expire_missing(company, {j["job_url"] for j in all_jobs})

    existing_urls = db.get_all_urls()
    to_enrich = [j for j in all_jobs if j["job_url"] not in existing_urls]
    to_update  = [j for j in all_jobs if j["job_url"] in existing_urls]
    logger.info(f"  🔍 {len(to_enrich)} nouvelles · {len(to_update)} mises à jour")

    # Récupérer les contract_types actuels en DB pour les offres existantes
    existing_contracts: Dict[str, str] = {}
    if to_update:
        with sqlite3.connect(db.db_path) as _conn:
            _urls = [j["job_url"] for j in to_update]
            ph = ",".join("?" * len(_urls))
            for row in _conn.execute(f"SELECT job_url, contract_type FROM jobs WHERE job_url IN ({ph})", _urls):
                existing_contracts[row[0]] = row[1] or ""

    for job in to_update:
        # Si contract_type vide en DB et pas extrait du listing → re-enrichir la page détail
        if not job.get("contract_type") and not existing_contracts.get(job["job_url"]):
            html = fetch_text(job["job_url"], session)
            if html:
                job = wp_enrich_detail(html, job)
            if not job.get("contract_type"):
                job["contract_type"] = "CDI"  # tous les postes WP KPMG sont des CDI permanents
            time.sleep(DETAIL_DELAY)
        apply_nlp(job)
        db.upsert(job)

    for i, job in enumerate(to_enrich, 1):
        logger.info(f"    [{i}/{len(to_enrich)}] {job['job_title'][:65]}")
        html = fetch_text(job["job_url"], session)
        if html:
            job = wp_enrich_detail(html, job)
        if not job.get("contract_type"):
            job["contract_type"] = "CDI"  # fallback CDI si employmentType absent du JSON-LD
        apply_nlp(job)
        db.upsert(job)
        time.sleep(DETAIL_DELAY)

    logger.info(f"  ✅ {company} : {len(to_enrich)} nouvelles · {len(to_update)} mises à jour")


# ═══════════════════════════════════════════════════════════════════════════════
#  MAIN
# ═══════════════════════════════════════════════════════════════════════════════
def run():
    db      = Database()
    session = requests.Session()
    results: Dict[str, str] = {}

    def _run(fn, sources):
        for source in sources:
            name = source["company_name"]
            try:
                fn(source, db, session)
                results[name] = "OK"
            except Exception as e:
                logger.error(f"  ❌ {name} : {e}", exc_info=True)
                results[name] = f"ERREUR: {e}"

    _run(scrape_radancy,          RADANCY_SOURCES)
    _run(scrape_smartrecruiters,  SMARTRECRUITERS_SOURCES)
    _run(scrape_lever,            LEVER_SOURCES)
    _run(scrape_lunr,             LUNR_SOURCES)
    _run(scrape_hrmanager,        HRMANAGER_SOURCES)
    _run(scrape_wordpress,        WORDPRESS_SOURCES)

    logger.info("\n" + "=" * 58)
    logger.info("  RÉSUMÉ KPMG GLOBAL")
    logger.info("=" * 58)
    ok  = [n for n, s in results.items() if s == "OK"]
    err = [n for n, s in results.items() if s != "OK"]
    for name in ok:
        logger.info(f"  ✅ {name}")
    for name in err:
        logger.info(f"  ❌ {name} : {results[name]}")

    if err:
        logger.warning(f"\n  {len(err)} marché(s) en erreur sur {len(results)}")


if __name__ == "__main__":
    run()
