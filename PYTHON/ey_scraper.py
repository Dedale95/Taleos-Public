#!/usr/bin/env python3
"""
EY — JOB SCRAPER
=================
Source : portail SAP SuccessFactors SiteBuilder (careers.ey.com)
  → sitemap.xml  — 1 requête → 8 700+ URLs + lastmod
  → detail HTML  — og:title + div.jobdescription

Phase 1 : sitemap.xml  → collecte URLs + date de publication
Phase 2 : detail HTML  → description, titre propre, localisation (parallèle)
Phase 3 : NLP backfill → éducation / expérience

Couverture : toutes régions mondiales (France, UK, Allemagne, USA,
             Inde, Espagne, Portugal, Belgique, Luxembourg, etc.)
             + Assurance, Tax, Consulting, Strategy & Transactions,
               EY-Parthenon, People Advisory, Tech…
"""

import logging
import re
import sqlite3
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from html import unescape
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple
from urllib.parse import unquote

import requests
from bs4 import BeautifulSoup

try:
    from city_normalizer import normalize_city
    from country_normalizer import normalize_country, get_country_from_city
    from job_family_classifier import classify_job_family
    from experience_extractor import extract_experience_level
    from education_extractor import extract_education_level
except ImportError:
    import sys
    sys.path.append(str(Path(__file__).parent))
    from city_normalizer import normalize_city
    from country_normalizer import normalize_country, get_country_from_city
    from job_family_classifier import classify_job_family
    from experience_extractor import extract_experience_level
    from education_extractor import extract_education_level

# ─────────────────────────── Config ─────────────────────────────

COMPANY_NAME    = "EY"
DB_PATH         = Path(__file__).parent / "ey_jobs.db"

SITEMAP_URL     = "https://careers.ey.com/sitemap.xml"
JOB_BASE_URL    = "https://careers.ey.com"

DETAIL_WORKERS  = 10      # threads parallèles pour les détails
DETAIL_DELAY    = 0.2     # délai entre requêtes par thread (s)
REQUEST_TIMEOUT = 20
MAX_DESC_CHARS  = 25_000

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept":          "text/html,application/xhtml+xml,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9,fr;q=0.8",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection":      "keep-alive",
}

# ─────────────────────────── Logging ────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════
# CONTRAT  —  inférence depuis le titre + description
# ═══════════════════════════════════════════════════════════════

def parse_contract_type(title: str, description: str = "") -> str:
    t = (title or "").lower()
    d = (description or "").lower()[:3000]

    # ── Inférence depuis le titre (haute priorité) ───────────
    if re.search(r"\bstage\b|\bstagiaire\b|\binternship\b|\bintern\b"
                 r"|\bpraktikum\b|\btirocinio\b|\bpraktikant\b", t):
        return "Stage"
    if re.search(r"\balternance\b|\balternant[e]?\b|\bapprentice\b"
                 r"|\bapprenti[e]?\b|\bausbildung\b|\bwerkstudent\b", t):
        return "Alternance / Apprentissage"
    if re.search(r"\bv\.?i\.?e\.?\b|\bvolontariat\s+international\b", t):
        return "V.I.E."
    if re.search(r"\bgraduate\s+programme?\b|\bgraduate\s+program\b"
                 r"|\bgraduate\s+hire\b|\bgraduate\s+entry\b", t):
        return "CDD"

    # ── Inférence depuis la description ─────────────────────
    if re.search(r"\bstage\b|\bstagiaire\b|\binternship\b|\bintern\b"
                 r"|\bpraktikum\b", d):
        return "Stage"
    if re.search(r"\balternance\b|\bapprentissage\b|\bcontrat\s+d.apprentissage\b"
                 r"|\bapprentice\b|\bausbildung\b", d):
        return "Alternance / Apprentissage"
    if re.search(r"\bv\.?i\.?e\.?\b|\bvolontariat\s+international\b", d):
        return "V.I.E."
    if re.search(r"\b(?:cdd|contrat\s+à\s+durée\s+déterminée|fixed.term"
                 r"|fixed.contract|temporary\s+contract)\b", d):
        return "CDD"

    return "CDI"


# ═══════════════════════════════════════════════════════════════
# LOCALISATION  —  parsing depuis og:description + city normalizer
# ═══════════════════════════════════════════════════════════════

# Codes d'états US/Canada souvent présents dans l'og:desc EY
_US_STATE_RE = re.compile(
    r"\b(AL|AK|AZ|AR|CA|CO|CT|DE|FL|GA|HI|ID|IL|IN|IA|KS|KY|LA|ME|MD|MA"
    r"|MI|MN|MS|MO|MT|NE|NV|NH|NJ|NM|NY|NC|ND|OH|OK|OR|PA|RI|SC|SD|TN"
    r"|TX|UT|VT|VA|WA|WV|WI|WY|DC|PR|GU|VI)\b"
)
_CA_PROV_RE = re.compile(
    r"\b(ON|QC|BC|AB|MB|SK|NS|NB|NL|PE|NT|YT|NU)\b"
)

# Codes postaux → pays (patterns, ordre du plus spécifique au moins)
_POSTAL_COUNTRY = [
    (re.compile(r"\d{5}-\d{3}\b"),       "Brésil"),          # NNNNN-NNN (BR)
    (re.compile(r"\b[A-Z]{1,2}\d[A-Z\d]?\s?\d[A-Z]{2}\b"), "Royaume-Uni"),  # UK postcode
    (re.compile(r"\b\d{4}\s?[A-Z]{2}\b"), "Pays-Bas"),        # NNNN AA (NL)
    (re.compile(r"\b\d{6}\b"),            "Inde"),             # 6 chiffres (IN PIN)
    (re.compile(r"\b[A-Z]\d[A-Z]\s?\d[A-Z]\d\b"), "Canada"), # A1A 1A1 (CA)
]

# Villes EY connues non couvertes par city_normalizer
_CITY_COUNTRY_MAP = {
    "gurugram": "Inde", "gurgaon": "Inde",
    "bengaluru": "Inde", "bangalore": "Inde",
    "hyderabad": "Inde", "pune": "Inde", "noida": "Inde",
    "ahmedabad": "Inde", "chennai": "Inde", "kolkata": "Inde",
    "new delhi": "Inde", "bombay": "Inde", "kochi": "Inde",
    "são paulo": "Brésil", "sao paulo": "Brésil",
    "rio de janeiro": "Brésil", "curitiba": "Brésil",
    "bogotá": "Colombie", "bogota": "Colombie",
    "medellín": "Colombie", "medellin": "Colombie",
    "santiago": "Chili",
    "lima": "Pérou",
    "buenos aires": "Argentine",
    "dubai": "Émirats Arabes Unis", "abu dhabi": "Émirats Arabes Unis",
    "riyadh": "Arabie Saoudite", "jeddah": "Arabie Saoudite",
    "düsseldorf": "Allemagne", "dusseldorf": "Allemagne",
    "frankfurt": "Allemagne", "münchen": "Allemagne", "munich": "Allemagne",
    "hamburg": "Allemagne", "berlin": "Allemagne", "cologne": "Allemagne",
    "köln": "Allemagne", "stuttgart": "Allemagne", "eschborn": "Allemagne",
    "zürich": "Suisse", "zurich": "Suisse", "geneva": "Suisse", "genève": "Suisse",
    "lausanne": "Suisse", "basel": "Suisse",
    "warsaw": "Pologne", "kraków": "Pologne", "katowice": "Pologne",
    "wroclaw": "Pologne", "wrocław": "Pologne", "poznan": "Pologne",
    "prague": "Tchéquie", "bratislava": "Slovaquie",
    "bucharest": "Roumanie", "budapest": "Hongrie",
    "sofia": "Bulgarie", "zagreb": "Croatie",
    "istanbul": "Turquie", "ankara": "Turquie",
    "tel aviv": "Israël",
    "johannesburg": "Afrique du Sud", "cape town": "Afrique du Sud",
    "nairobi": "Kenya", "lagos": "Nigeria", "accra": "Ghana",
    "casablanca": "Maroc", "tunis": "Tunisie", "alger": "Algérie",
    "manila": "Philippines", "taguig": "Philippines", "makati": "Philippines",
    "jakarta": "Indonésie",
    "kuala lumpur": "Malaisie", "bangkok": "Thaïlande",
    "ho chi minh": "Vietnam", "hanoi": "Vietnam",
    "seoul": "Corée du Sud", "tokyo": "Japon", "osaka": "Japon",
    "taipei": "Taïwan",
    "shanghai": "Chine", "beijing": "Chine", "shenzhen": "Chine",
    "guangzhou": "Chine", "chengdu": "Chine",
    "hong kong": "Hong-Kong", "macau": "Macao", "macao": "Macao",
    "auckland": "Nouvelle-Zélande", "wellington": "Nouvelle-Zélande",
    "melbourne": "Australie", "sydney": "Australie", "brisbane": "Australie",
    "perth": "Australie", "canberra": "Australie",
    # Philippines districts
    "youngdeungpo": "Corée du Sud", "youngdeungpo-gu": "Corée du Sud",
    # Amérique Latine
    "ciudad de mexico": "Mexique", "monterrey": "Mexique",
    "panama": "Panama", "bogota": "Colombie",
    "santiago de chile": "Chili",
    # Autres
    "amsterdam": "Pays-Bas", "rotterdam": "Pays-Bas",
    "brussels": "Belgique", "bruxelles": "Belgique",
    "london": "Royaume-Uni", "manchester": "Royaume-Uni", "edinburgh": "Royaume-Uni",
    "dublin": "Irlande",
    "oslo": "Norvège", "stockholm": "Suède", "helsinki": "Finlande",
    "copenhagen": "Danemark", "vienna": "Autriche",
    "rome": "Italie", "milan": "Italie", "madrid": "Espagne", "barcelona": "Espagne",
    "lisbon": "Portugal", "athens": "Grèce",
    "singapore": "Singapour",
    "karachi": "Pakistan", "lahore": "Pakistan", "islamabad": "Pakistan",
    "colombo": "Sri Lanka",
    "amman": "Jordanie", "beirut": "Liban",
    "doha": "Qatar", "kuwait city": "Koweït", "manama": "Bahreïn",
    "moscow": "Russie", "kyiv": "Ukraine",
}

# Codes de bureau EY internes parfois collés à la ville dans og:desc (ex: "São Paulo JK")
_OFFICE_CODE_RE = re.compile(r"\s+[A-Z]{1,3}$")


def _country_from_postal(og_desc: str) -> str:
    """Détecte le pays depuis le code postal dans og:desc."""
    for pattern, country in _POSTAL_COUNTRY:
        if pattern.search(og_desc or ""):
            return country
    return ""


def _city_country_from_text(text: str) -> Tuple[str, str]:
    """
    Tente d'extraire (city_norm, country_norm) depuis les N premiers mots de `text`.
    Teste des combinaisons de 1, 2, 3 mots contre _CITY_COUNTRY_MAP et city_normalizer.
    Retourne ("", "") si aucun match.
    """
    if not text:
        return "", ""
    words = text.strip().split()
    for n in range(min(3, len(words)), 0, -1):
        candidate = " ".join(words[:n])
        candidate_low = candidate.lower()
        # Table directe
        country = _CITY_COUNTRY_MAP.get(candidate_low, "")
        if not country:
            # city_normalizer
            cn = normalize_city(candidate)
            cg = get_country_from_city(cn) or get_country_from_city(candidate_low)
            country = normalize_country(cg or "") or ""
        if country:
            city_norm = normalize_city(candidate) or candidate.title()
            return city_norm, country
    return "", ""


def parse_location(og_title: str, og_desc: str) -> Tuple[str, str, str]:
    """
    Retourne (location_display, country_normalized, city_normalized).
    og:desc EY  = "{city} {og_title}, {zipOrState}"
    og:title EY = "{job title}"
    """
    city_raw = ""
    if og_title and og_desc and og_title in og_desc:
        # city = partie de og_desc avant le og_title
        pre = og_desc.split(og_title, 1)[0].strip()
        city_raw = pre.strip()

    if not city_raw:
        # Fallback : og_title commence parfois par la ville (pattern EY : "Bengaluru Risk Consulting…")
        # Ou tenter depuis le début de og_desc si og_title absent
        for src in (og_title or "", og_desc or ""):
            city_try, country_try = _city_country_from_text(src)
            if country_try:
                display = f"{city_try} - {country_try}"
                return display, country_try, city_try
        return "", "", ""

    # Retire les codes de bureau EY internes (ex: " JK", " GDS")
    city_clean = _OFFICE_CODE_RE.sub("", city_raw).strip()
    city_clean = re.sub(r"[,\-\s]+$", "", city_clean).strip()

    # Normalise la ville
    city_norm = normalize_city(city_clean) or city_clean

    # 1. Cherche le pays via la ville (city_normalizer)
    country_guess = get_country_from_city(city_norm) or get_country_from_city(city_clean)
    country_norm  = normalize_country(country_guess or "") or ""

    # 2. Fallback : table de villes EY connues
    if not country_norm:
        country_norm = _CITY_COUNTRY_MAP.get(city_norm.lower(), "") or \
                       _CITY_COUNTRY_MAP.get(city_clean.lower(), "")

    # 3. Fallback : code postal dans og_desc
    if not country_norm:
        country_norm = _country_from_postal(og_desc or "")

    # 4. Fallback : code état US/Canada dans og_desc
    if not country_norm:
        if _US_STATE_RE.search(og_desc or ""):
            country_norm = "États-Unis"
        elif _CA_PROV_RE.search(og_desc or ""):
            country_norm = "Canada"

    display = f"{city_norm} - {country_norm}" if country_norm else city_norm

    return display, country_norm, city_norm


# ═══════════════════════════════════════════════════════════════
# DATABASE
# ═══════════════════════════════════════════════════════════════

class Database:
    def __init__(self, db_path: Path = DB_PATH):
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
                    country              TEXT,
                    region               TEXT,
                    job_family           TEXT,
                    work_style           TEXT,
                    management_position  TEXT DEFAULT 'Non',
                    status               TEXT DEFAULT 'Live',
                    education_level      TEXT,
                    experience_level     TEXT,
                    job_description      TEXT,
                    company_name         TEXT DEFAULT 'EY',
                    source               TEXT DEFAULT 'EY',
                    sf_id                TEXT,
                    is_valid             INTEGER DEFAULT 1,
                    first_seen           TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    last_updated         TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            conn.execute("CREATE INDEX IF NOT EXISTS idx_sf_id ON jobs(sf_id)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_status ON jobs(status)")
            conn.commit()
        self._migrate()

    def _migrate(self):
        """Supprime le UNIQUE constraint sur sf_id s'il existe (migration DB existante)."""
        with sqlite3.connect(self.db_path) as conn:
            # Chercher un index UNIQUE portant sur sf_id
            indices = conn.execute("PRAGMA index_list(jobs)").fetchall()
            sf_id_unique_index = None
            for idx in indices:
                idx_name, idx_unique = idx[1], idx[2]
                if idx_unique:
                    cols = conn.execute(f"PRAGMA index_info('{idx_name}')").fetchall()
                    if any(col[2] == 'sf_id' for col in cols):
                        sf_id_unique_index = idx_name
                        break
            if sf_id_unique_index is None:
                return  # Déjà migré ou table nouvelle
            logger.info(f"Migration DB : suppression index UNIQUE sur sf_id ({sf_id_unique_index})")
            # Recréer la table sans UNIQUE sur sf_id
            conn.execute("ALTER TABLE jobs RENAME TO _jobs_bak")
            conn.execute("""
                CREATE TABLE jobs (
                    job_url              TEXT PRIMARY KEY,
                    job_id               TEXT,
                    job_title            TEXT,
                    contract_type        TEXT,
                    publication_date     TEXT,
                    location             TEXT,
                    country              TEXT,
                    region               TEXT,
                    job_family           TEXT,
                    work_style           TEXT,
                    management_position  TEXT DEFAULT 'Non',
                    status               TEXT DEFAULT 'Live',
                    education_level      TEXT,
                    experience_level     TEXT,
                    job_description      TEXT,
                    company_name         TEXT DEFAULT 'EY',
                    source               TEXT DEFAULT 'EY',
                    sf_id                TEXT,
                    is_valid             INTEGER DEFAULT 1,
                    first_seen           TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    last_updated         TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            conn.execute("INSERT INTO jobs SELECT * FROM _jobs_bak")
            conn.execute("DROP TABLE _jobs_bak")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_sf_id ON jobs(sf_id)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_status ON jobs(status)")
            conn.commit()
            logger.info("Migration DB EY terminée")

    def upsert(self, job: Dict) -> bool:
        """Insert ou met à jour. Retourne True si nouvelle offre."""
        url = job.get("job_url", "")
        if not url:
            return False
        with sqlite3.connect(self.db_path) as conn:
            existing = conn.execute(
                "SELECT job_url FROM jobs WHERE job_url = ?", (url,)
            ).fetchone()
            is_new = existing is None
            # Neutraliser sf_id si déjà pris par un autre job_url (évite tout UNIQUE résiduel)
            sf_id = job.get("sf_id", "") or ""
            if sf_id:
                conflict = conn.execute(
                    "SELECT job_url FROM jobs WHERE sf_id = ? AND job_url != ?", (sf_id, url)
                ).fetchone()
                if conflict:
                    sf_id = None
            params = {
                "job_url":            job.get("job_url", ""),
                "job_id":             job.get("job_id", ""),
                "job_title":          job.get("job_title", ""),
                "contract_type":      job.get("contract_type", ""),
                "publication_date":   job.get("publication_date", ""),
                "location":           job.get("location", ""),
                "country":            job.get("country", ""),
                "region":             job.get("region", ""),
                "job_family":         job.get("job_family", ""),
                "work_style":         job.get("work_style", ""),
                "management_position": job.get("management_position", "Non"),
                "status":             job.get("status", "Live"),
                "education_level":    job.get("education_level", ""),
                "experience_level":   job.get("experience_level", ""),
                "job_description":    job.get("job_description", ""),
                "company_name":       COMPANY_NAME,
                "source":             "EY",
                "sf_id":              sf_id,
                "is_valid":           job.get("is_valid", 1),
            }
            try:
                conn.execute("""
                    INSERT INTO jobs (
                        job_url, job_id, job_title, contract_type, publication_date,
                        location, country, region, job_family, work_style,
                        management_position, status, education_level, experience_level,
                        job_description, company_name, source, sf_id, is_valid, last_updated
                    ) VALUES (
                        :job_url, :job_id, :job_title, :contract_type, :publication_date,
                        :location, :country, :region, :job_family, :work_style,
                        :management_position, :status, :education_level, :experience_level,
                        :job_description, :company_name, :source, :sf_id, :is_valid,
                        CURRENT_TIMESTAMP
                    )
                    ON CONFLICT(job_url) DO UPDATE SET
                        job_title        = excluded.job_title,
                        contract_type    = CASE WHEN excluded.contract_type != ''
                                               THEN excluded.contract_type
                                               ELSE jobs.contract_type END,
                        publication_date = CASE WHEN excluded.publication_date != ''
                                               THEN excluded.publication_date
                                               ELSE jobs.publication_date END,
                        location         = CASE WHEN excluded.location != ''
                                               THEN excluded.location
                                               ELSE jobs.location END,
                        country          = CASE WHEN excluded.country != ''
                                               THEN excluded.country
                                               ELSE jobs.country END,
                        region           = CASE WHEN excluded.region != ''
                                               THEN excluded.region
                                               ELSE jobs.region END,
                        job_family       = CASE WHEN excluded.job_family != ''
                                               THEN excluded.job_family
                                               ELSE jobs.job_family END,
                        status           = excluded.status,
                        education_level  = COALESCE(NULLIF(excluded.education_level, ''),
                                                   jobs.education_level),
                        experience_level = COALESCE(NULLIF(excluded.experience_level, ''),
                                                   jobs.experience_level),
                        job_description  = COALESCE(NULLIF(excluded.job_description, ''),
                                                   jobs.job_description),
                        last_updated     = CURRENT_TIMESTAMP,
                        is_valid         = excluded.is_valid
                """, params)
                conn.commit()
            except sqlite3.IntegrityError as e:
                if "sf_id" in str(e).lower():
                    logger.warning(f"sf_id conflict ignoré pour {url}: {e}")
                else:
                    raise
        return is_new

    def get_live_urls(self) -> Set[str]:
        with sqlite3.connect(self.db_path) as conn:
            rows = conn.execute(
                "SELECT job_url FROM jobs WHERE status = 'Live'"
            ).fetchall()
        return {r[0] for r in rows}

    def get_urls_without_description(self) -> List[Tuple[str, str]]:
        """Retourne (job_url, sf_id) pour les offres Live sans description."""
        with sqlite3.connect(self.db_path) as conn:
            rows = conn.execute(
                "SELECT job_url, sf_id FROM jobs WHERE status = 'Live' "
                "AND (job_description IS NULL OR job_description = '')"
            ).fetchall()
        return [(r[0], r[1]) for r in rows]

    def mark_expired(self, urls: Set[str]):
        if not urls:
            return
        with sqlite3.connect(self.db_path) as conn:
            ph = ",".join("?" * len(urls))
            conn.execute(
                f"UPDATE jobs SET status='Expired', last_updated=CURRENT_TIMESTAMP "
                f"WHERE job_url IN ({ph})",
                tuple(urls),
            )
            conn.commit()

    def update_detail(self, job_url: str, data: Dict):
        """Met à jour les champs issus du scraping détail."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                UPDATE jobs SET
                    job_title        = CASE WHEN ? != '' THEN ? ELSE job_title END,
                    contract_type    = CASE WHEN ? != '' THEN ? ELSE contract_type END,
                    location         = CASE WHEN ? != '' THEN ? ELSE location END,
                    country          = CASE WHEN ? != '' THEN ? ELSE country END,
                    region           = CASE WHEN ? != '' THEN ? ELSE region END,
                    job_family       = CASE WHEN ? != '' THEN ? ELSE job_family END,
                    management_position = ?,
                    education_level  = COALESCE(NULLIF(?, ''), education_level),
                    experience_level = COALESCE(NULLIF(?, ''), experience_level),
                    job_description  = COALESCE(NULLIF(?, ''), job_description),
                    last_updated     = CURRENT_TIMESTAMP
                WHERE job_url = ?
            """, (
                data.get("job_title", ""),     data.get("job_title", ""),
                data.get("contract_type", ""), data.get("contract_type", ""),
                data.get("location", ""),      data.get("location", ""),
                data.get("country", ""),       data.get("country", ""),
                data.get("region", ""),        data.get("region", ""),
                data.get("job_family", ""),    data.get("job_family", ""),
                data.get("management_position", "Non"),
                data.get("education_level", ""),
                data.get("experience_level", ""),
                data.get("job_description", ""),
                job_url,
            ))
            conn.commit()

    def count_live(self) -> int:
        with sqlite3.connect(self.db_path) as conn:
            return conn.execute(
                "SELECT COUNT(*) FROM jobs WHERE status='Live'"
            ).fetchone()[0]

    def backfill_education_experience(self) -> int:
        """Ré-infère éducation + expérience pour les offres sans valeur."""
        with sqlite3.connect(self.db_path) as conn:
            rows = conn.execute(
                "SELECT job_url, job_title, job_description, contract_type "
                "FROM jobs WHERE status='Live' AND ("
                "  education_level IS NULL OR education_level = '' OR "
                "  experience_level IS NULL OR experience_level = '')"
            ).fetchall()
            updated = 0
            for url, title, desc, ct in rows:
                edu = extract_education_level(desc or "", ct or "", title or "")
                exp = extract_experience_level(desc or "", ct or "", title or "")
                if edu or exp:
                    conn.execute(
                        "UPDATE jobs SET "
                        "education_level  = COALESCE(NULLIF(education_level, ''),  ?), "
                        "experience_level = COALESCE(NULLIF(experience_level, ''), ?), "
                        "last_updated = CURRENT_TIMESTAMP WHERE job_url = ?",
                        (edu or None, exp or None, url),
                    )
                    updated += 1
            conn.commit()
        return updated


# ═══════════════════════════════════════════════════════════════
# PHASE 1 — SITEMAP
# ═══════════════════════════════════════════════════════════════

def fetch_sitemap(session: requests.Session) -> List[Tuple[str, str]]:
    """
    Récupère le sitemap.xml et retourne la liste de (job_url, lastmod).
    """
    try:
        resp = session.get(SITEMAP_URL, headers=HEADERS, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
    except Exception as e:
        logger.error(f"Sitemap — erreur : {e}")
        return []

    xml = resp.text
    urls   = re.findall(r"<loc>(https://careers\.ey\.com/ey/job/[^<]+)</loc>", xml)
    dates  = re.findall(r"<lastmod>([^<]+)</lastmod>", xml)

    # Decode HTML entities in URLs
    urls = [unescape(u) for u in urls]

    # Aligner dates (peut être moins que les URLs si certaines n'ont pas de lastmod)
    pairs = []
    for i, url in enumerate(urls):
        date = dates[i] if i < len(dates) else ""
        pairs.append((url, date))

    logger.info(f"Sitemap — {len(pairs)} offres trouvées")
    return pairs


def _extract_sf_id_from_url(url: str) -> str:
    """Extrait l'ID SuccessFactors numérique de l'URL EY."""
    m = re.search(r"/(\d{8,12})/?$", url)
    return m.group(1) if m else ""


def _title_from_url(url: str) -> str:
    """
    Extrait un titre approximatif depuis le slug URL pour la Phase 1
    (avant que le détail HTML soit scraped).
    Format : /ey/job/{City-Title-Words}/{sf_id}/
    """
    slug = url.rstrip("/").rsplit("/", 2)[-2] if "/" in url else ""
    slug = unquote(slug).replace("-", " ").replace("_", " ")
    # Retire le code postal/état à la fin (derniers tokens numériques ou code état)
    slug = re.sub(r"\s+\d{4,6}$", "", slug).strip()
    return slug[:200]


def transform_sitemap_entry(url: str, lastmod: str) -> Dict:
    """
    Construit un dict DB minimal depuis une entrée sitemap (Phase 1).
    Les champs manquants (description, titre propre, localisation) seront
    enrichis en Phase 2.
    """
    sf_id     = _extract_sf_id_from_url(url)
    title_raw = _title_from_url(url)

    # Contrat approximatif depuis le slug URL
    ct = parse_contract_type(title_raw)

    return {
        "job_url":           url,
        "job_id":            sf_id,
        "job_title":         title_raw,
        "contract_type":     ct,
        "publication_date":  lastmod[:10] if lastmod else "",
        "location":          "",
        "country":           "",
        "region":            "",
        "job_family":        "",
        "work_style":        "",
        "management_position": "Non",
        "status":            "Live",
        "education_level":   "",
        "experience_level":  "",
        "job_description":   "",
        "sf_id":             sf_id,
        "is_valid":          1,
    }


# ═══════════════════════════════════════════════════════════════
# PHASE 2 — DÉTAIL HTML
# ═══════════════════════════════════════════════════════════════

def _html_to_text(html_or_tag) -> str:
    """Convertit un tag BS4 ou HTML brut en texte propre."""
    if html_or_tag is None:
        return ""
    if isinstance(html_or_tag, str):
        soup = BeautifulSoup(html_or_tag, "html.parser")
        text = soup.get_text(separator=" ", strip=True)
    else:
        text = html_or_tag.get_text(separator=" ", strip=True)
    text = re.sub(r"\s+", " ", text).strip()
    return text[:MAX_DESC_CHARS]


def _is_management(title: str) -> bool:
    return bool(re.search(
        r"\b(?:director|directeur|manager|responsable|head\s+of|vp\b|vice.president"
        r"|managing\s+director|partner|chief|président|associé)\b",
        (title or "").lower()
    ))


def fetch_detail(session: requests.Session, job_url: str) -> Optional[Dict]:
    """
    Scrape la page HTML détail d'un job EY.
    Retourne un dict avec les champs enrichis, ou None en cas d'erreur.
    """
    try:
        resp = session.get(job_url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
    except Exception as e:
        logger.debug(f"Détail {job_url[-60:]} — erreur : {e}")
        return None

    soup = BeautifulSoup(resp.text, "html.parser")

    # ── Titre ──────────────────────────────────────────────────
    og_title_tag  = soup.find("meta", property="og:title")
    tw_title_tag  = soup.find("meta", attrs={"name": "twitter:title"})
    h1_tag        = soup.find("h1")
    og_title = ""
    for tag in (og_title_tag, tw_title_tag):
        if tag and tag.get("content", "").strip():
            og_title = tag["content"].strip()
            break
    if not og_title and h1_tag:
        og_title = h1_tag.get_text(strip=True)
    title = og_title.strip()

    # ── Localisation ────────────────────────────────────────────
    desc_meta_tag = (
        soup.find("meta", attrs={"name": "description"})
        or soup.find("meta", property="og:description")
    )
    og_desc = (desc_meta_tag.get("content", "") if desc_meta_tag else "").strip()
    location_display, country, city = parse_location(title, og_desc)

    # ── Description ─────────────────────────────────────────────
    desc_div = (
        soup.find(class_=re.compile(r"jobdescription", re.I))
        or soup.find(class_=re.compile(r"\bdescription\b", re.I))
    )
    desc_text = _html_to_text(desc_div)

    # ── Contrat ─────────────────────────────────────────────────
    contract_type = parse_contract_type(title, desc_text)

    # ── Famille de métier ───────────────────────────────────────
    job_family = classify_job_family(title, desc_text)

    # ── NLP éducation / expérience ──────────────────────────────
    education_level  = extract_education_level(desc_text, contract_type, title)
    experience_level = extract_experience_level(desc_text, contract_type, title)

    # ── Management ──────────────────────────────────────────────
    is_mgmt = _is_management(title)

    return {
        "job_title":          title,
        "contract_type":      contract_type,
        "location":           location_display,
        "country":            country,
        "region":             city,
        "job_family":         job_family or "",
        "management_position": "Oui" if is_mgmt else "Non",
        "education_level":    education_level or "",
        "experience_level":   experience_level or "",
        "job_description":    desc_text,
    }


def enrich_jobs_parallel(db: Database, urls_to_enrich: List[str]) -> int:
    """
    Enrichit les offres sans description en parallèle via ThreadPoolExecutor.
    """
    if not urls_to_enrich:
        return 0

    logger.info(f"Phase 2 — {len(urls_to_enrich)} offres à enrichir ({DETAIL_WORKERS} threads)")

    done  = 0
    total = len(urls_to_enrich)

    # Session par thread (thread-safe avec un lock pour la DB)
    def _worker(url: str) -> Tuple[str, Optional[Dict]]:
        s = requests.Session()
        time.sleep(DETAIL_DELAY)
        detail = fetch_detail(s, url)
        return url, detail

    with ThreadPoolExecutor(max_workers=DETAIL_WORKERS) as executor:
        futures = {executor.submit(_worker, url): url for url in urls_to_enrich}
        for fut in as_completed(futures):
            url, detail = fut.result()
            if detail:
                db.update_detail(url, detail)
            done += 1
            if done % 200 == 0 or done == total:
                logger.info(f"  Phase 2 : {done}/{total} ({done/total*100:.0f}%)")

    return done


# ═══════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════

def main():
    db      = Database(DB_PATH)
    session = requests.Session()
    t0      = time.time()

    # ── Phase 1 : sitemap → liste complète ──────────────────
    logger.info("=" * 60)
    logger.info("Phase 1 — Sitemap XML → liste des offres")
    logger.info("=" * 60)

    sitemap_entries = fetch_sitemap(session)
    if not sitemap_entries:
        logger.error("Phase 1 échouée — sitemap vide. Arrêt.")
        return

    sitemap_urls = {url for url, _ in sitemap_entries}

    # Marquer Expired les offres absentes du sitemap
    live_db_urls  = db.get_live_urls()
    expired_urls  = live_db_urls - sitemap_urls
    if expired_urls:
        db.mark_expired(expired_urls)
        logger.info(f"  → {len(expired_urls)} offres expirées marquées")

    # Upsert toutes les entrées sitemap (Phase 1 = infos minimales)
    new_count = 0
    for url, lastmod in sitemap_entries:
        job_dict = transform_sitemap_entry(url, lastmod)
        is_new   = db.upsert(job_dict)
        if is_new:
            new_count += 1

    logger.info(f"Phase 1 — {len(sitemap_entries)} offres upsertées ({new_count} nouvelles)")

    # ── Phase 2 : enrichissement HTML (delta) ───────────────
    urls_to_enrich = [url for url, _ in db.get_urls_without_description()]
    if urls_to_enrich:
        logger.info("=" * 60)
        logger.info(f"Phase 2 — Scraping détail HTML ({len(urls_to_enrich)} offres)")
        logger.info("=" * 60)
        enrich_jobs_parallel(db, urls_to_enrich)
        logger.info("Phase 2 terminée")
    else:
        logger.info("Phase 2 — aucune nouvelle offre à enrichir")

    # ── Phase 3 : backfill NLP ──────────────────────────────
    updated = db.backfill_education_experience()
    if updated:
        logger.info(f"Phase 3 — {updated} offres enrichies (éducation/expérience)")

    # ── Stats finales ────────────────────────────────────────
    total_live = db.count_live()
    elapsed    = time.time() - t0
    logger.info("=" * 60)
    logger.info(f"✅ EY — {total_live} offres Live en base")
    logger.info(f"   Durée  : {elapsed:.0f}s")
    logger.info(f"   Base   : {DB_PATH}")
    logger.info("=" * 60)

    with sqlite3.connect(DB_PATH) as conn:
        by_country = conn.execute(
            "SELECT country, COUNT(*) as n FROM jobs WHERE status='Live' "
            "GROUP BY country ORDER BY n DESC LIMIT 15"
        ).fetchall()
    logger.info("Top 15 pays :")
    for country, n in by_country:
        logger.info(f"  {country or 'Non spécifié':<30} {n}")


if __name__ == "__main__":
    main()
