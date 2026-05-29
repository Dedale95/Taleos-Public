#!/usr/bin/env python3
"""
NOMURA — JOB SCRAPER
====================
Deux sources :
  1. careers.nomura.com  (SAP SuccessFactors CSB) — Experienced professionals
     Entités : Nomura, Instinet, Nomura Greentech, Laser
     Méthode  : sitemap.xml (1 000+ URLs) → détail HTML par offre

  2. nomuracampus.tal.net (Lumesse TalentLink) — Students & Graduates
     Méthode  : listing HTML → détail HTML par offre

Champs extraits (aucun "Non spécifié" accepté si la donnée existe sur la page) :
  job_title, entity, candidate_type, city, country, region,
  contract_type, experience_level, education_level, division,
  job_family, job_family_level2, offer_url, description,
  posted_date, closing_date, scraped_at
"""

from __future__ import annotations

import logging
import re
import sqlite3
import time
import xml.etree.ElementTree as ET
from datetime import datetime
from pathlib import Path
from typing import Optional
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

try:
    from country_normalizer import normalize_country
    from job_family_classifier import classify_job_family
    from experience_extractor import extract_experience_level
    from education_extractor import extract_education_level
except ImportError:
    import sys
    sys.path.append(str(Path(__file__).parent))
    from country_normalizer import normalize_country
    from job_family_classifier import classify_job_family
    from experience_extractor import extract_experience_level
    from education_extractor import extract_education_level

# ─────────────────────────── Logging ────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("nomura_scraper.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger(__name__)

# ─────────────────────────── Config ─────────────────────────────
DB_PATH        = Path(__file__).parent / "nomura_jobs.db"
SITEMAP_URL    = "https://careers.nomura.com/sitemap.xml"
SF_BASE        = "https://careers.nomura.com"
CAMPUS_LIST    = "https://nomuracampus.tal.net/vx/lang-en-GB/mobile-0/appcentre-ext/brand-4/xf-3348347fc789/candidate/jobboard/vacancy/1/adv/"
CAMPUS_DETAIL  = "https://nomuracampus.tal.net/vx/lang-en-GB/mobile-0/appcentre-1/brand-4/xf-5949f914ab38/candidate/so/pm/1/pl/1/opp/{slug}/en-GB"
COMPANY_NAME   = "Nomura"
REQUEST_DELAY  = 0.7    # secondes entre requêtes
REQUEST_TIMEOUT = 25
MAX_RETRIES    = 3

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept":          "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-GB,en;q=0.9,fr;q=0.8",
}

# ═══════════════════════════════════════════════════════════════
# MAPPINGS — pays, région, niveau, contrat
# ═══════════════════════════════════════════════════════════════

# Code ISO 2 lettres → (pays en français, région)
COUNTRY_CODE_MAP: dict[str, tuple[str, str]] = {
    # Europe (EMEA)
    "GB": ("Royaume-Uni",          "Europe"),
    "DE": ("Allemagne",            "Europe"),
    "FR": ("France",               "Europe"),
    "CH": ("Suisse",               "Europe"),
    "IE": ("Irlande",              "Europe"),
    "IT": ("Italie",               "Europe"),
    "NL": ("Pays-Bas",             "Europe"),
    "ES": ("Espagne",              "Europe"),
    "PL": ("Pologne",              "Europe"),
    "LU": ("Luxembourg",           "Europe"),
    "SE": ("Suède",                "Europe"),
    "DK": ("Danemark",             "Europe"),
    "AT": ("Autriche",             "Europe"),
    "PT": ("Portugal",             "Europe"),
    "BE": ("Belgique",             "Europe"),
    "CZ": ("République tchèque",   "Europe"),
    "HU": ("Hongrie",              "Europe"),
    "GR": ("Grèce",                "Europe"),
    "RO": ("Roumanie",             "Europe"),
    "SK": ("Slovaquie",            "Europe"),
    "FI": ("Finlande",             "Europe"),
    "NO": ("Norvège",              "Europe"),
    # EMEA hors UE
    "TR": ("Turquie",              "Europe"),
    "IL": ("Israël",               "Europe"),
    "AE": ("Émirats arabes unis",  "Europe"),
    "SA": ("Arabie Saoudite",      "Europe"),
    "QA": ("Qatar",                "Europe"),
    "BH": ("Bahreïn",              "Europe"),
    "ZA": ("Afrique du Sud",       "Europe"),
    "EG": ("Égypte",               "Europe"),
    "KE": ("Kenya",                "Europe"),
    "NG": ("Nigeria",              "Europe"),
    # Amériques
    "US": ("États-Unis",           "Amérique du Nord"),
    "CA": ("Canada",               "Amérique du Nord"),
    "BR": ("Brésil",               "Amérique du Nord"),
    "MX": ("Mexique",              "Amérique du Nord"),
    "AR": ("Argentine",            "Amérique du Nord"),
    "CL": ("Chili",                "Amérique du Nord"),
    "CO": ("Colombie",             "Amérique du Nord"),
    # Asie-Pacifique
    "JP": ("Japon",                "Asie-Pacifique"),
    "HK": ("Hong Kong",            "Asie-Pacifique"),
    "SG": ("Singapour",            "Asie-Pacifique"),
    "AU": ("Australie",            "Asie-Pacifique"),
    "CN": ("Chine",                "Asie-Pacifique"),
    "KR": ("Corée du Sud",         "Asie-Pacifique"),
    "TH": ("Thaïlande",            "Asie-Pacifique"),
    "MY": ("Malaisie",             "Asie-Pacifique"),
    "PH": ("Philippines",          "Asie-Pacifique"),
    "TW": ("Taïwan",               "Asie-Pacifique"),
    "ID": ("Indonésie",            "Asie-Pacifique"),
    "VN": ("Vietnam",              "Asie-Pacifique"),
    "NZ": ("Nouvelle-Zélande",     "Asie-Pacifique"),
    # Inde — traitée comme région distincte chez Nomura
    "IN": ("Inde",                 "Asie-Pacifique"),
}

# Ville → (pays, région) — pour les cas où le country code est absent/ambigu
CITY_COUNTRY_MAP: dict[str, tuple[str, str]] = {
    # Nomura HQ districts
    "central":      ("Hong Kong",       "Asie-Pacifique"),  # district HK
    "hong kong":    ("Hong Kong",       "Asie-Pacifique"),
    "london":       ("Royaume-Uni",     "Europe"),
    "frankfurt":    ("Allemagne",       "Europe"),
    "paris":        ("France",          "Europe"),
    "zurich":       ("Suisse",          "Europe"),
    "milan":        ("Italie",          "Europe"),
    "madrid":       ("Espagne",         "Europe"),
    "amsterdam":    ("Pays-Bas",        "Europe"),
    "warsaw":       ("Pologne",         "Europe"),
    "dublin":       ("Irlande",         "Europe"),
    "luxembourg":   ("Luxembourg",      "Europe"),
    "stockholm":    ("Suède",           "Europe"),
    "vienna":       ("Autriche",        "Europe"),
    "istanbul":     ("Turquie",         "Europe"),
    "dubai":        ("Émirats arabes unis", "Europe"),
    "abu dhabi":    ("Émirats arabes unis", "Europe"),
    "riyadh":       ("Arabie Saoudite", "Europe"),
    "doha":         ("Qatar",           "Europe"),
    "johannesburg": ("Afrique du Sud",  "Europe"),
    "new york":     ("États-Unis",      "Amérique du Nord"),
    "chicago":      ("États-Unis",      "Amérique du Nord"),
    "los angeles":  ("États-Unis",      "Amérique du Nord"),
    "san francisco":("États-Unis",      "Amérique du Nord"),
    "toronto":      ("Canada",          "Amérique du Nord"),
    "são paulo":    ("Brésil",          "Amérique du Nord"),
    "sao paulo":    ("Brésil",          "Amérique du Nord"),
    "tokyo":        ("Japon",           "Asie-Pacifique"),
    "osaka":        ("Japon",           "Asie-Pacifique"),
    "singapore":    ("Singapour",       "Asie-Pacifique"),
    "sydney":       ("Australie",       "Asie-Pacifique"),
    "melbourne":    ("Australie",       "Asie-Pacifique"),
    "beijing":      ("Chine",           "Asie-Pacifique"),
    "shanghai":     ("Chine",           "Asie-Pacifique"),
    "shenzhen":     ("Chine",           "Asie-Pacifique"),
    "seoul":        ("Corée du Sud",    "Asie-Pacifique"),
    "taipei":       ("Taïwan",          "Asie-Pacifique"),
    "kuala lumpur": ("Malaisie",        "Asie-Pacifique"),
    "bangkok":      ("Thaïlande",       "Asie-Pacifique"),
    "jakarta":      ("Indonésie",       "Asie-Pacifique"),
    "manila":       ("Philippines",     "Asie-Pacifique"),
    "mumbai":       ("Inde",            "Asie-Pacifique"),
    "bangalore":    ("Inde",            "Asie-Pacifique"),
    "bengaluru":    ("Inde",            "Asie-Pacifique"),
    "pune":         ("Inde",            "Asie-Pacifique"),
    "chennai":      ("Inde",            "Asie-Pacifique"),
    "hyderabad":    ("Inde",            "Asie-Pacifique"),
    "delhi":        ("Inde",            "Asie-Pacifique"),
    "gurugram":     ("Inde",            "Asie-Pacifique"),
    "gurgaon":      ("Inde",            "Asie-Pacifique"),
}

# Niveau d'expérience — extrait du titre (ordre du plus spécifique au plus large)
LEVEL_PATTERNS: list[tuple[str, str]] = [
    (r"\bManaging\s+Director\b|\bMD\b(?!\s+Anderson|\s+degree)|_MD\b",  "Managing Director"),
    (r"\bExecutive\s+Director\b|\bED\b|_ED\b",                          "Executive Director"),
    (r"\bSenior\s+Vice\s+President\b|\bSVP\b|_SVP\b",                   "Senior Vice President"),
    (r"\bVice\s+President\b|\bVP\b|_VP\b",                              "Vice President"),
    (r"\bSenior\s+Associate\b|_SA\b",                                   "Senior Associate"),
    (r"\bAssociate\s+Director\b|_AD\b",                                 "Associate Director"),
    (r"\bDirector\b|_DIR\b",                                            "Director"),
    (r"\bSenior\s+Analyst\b",                                           "Senior Analyst"),
    # Nomura India format : suffixe _AS (Associate) ou préfixe Asc./Assoc
    (r"\bAssociate\b|\bAsc\.(?!\w)|\bAssoc[-\s]|_AS\b",               "Associate"),
    # Nomura India format : suffixe _AN (Analyst)
    (r"\bAnalyst\b|_AN\b",                                              "Analyst"),
    (r"\bIntern(?:ship)?\b|\bOff[\s-]Cycle\b|\bSummer\s+Analyst\b|\bWorking\s+Student\b|\bWork\s+Placement\b",
                                                                        "Intern / Stage"),
]

# Contract type — SF experienced (defaults CDI, détection CDD/Intérim/Stage)
CONTRACT_EXPERIENCED_PATTERNS: list[tuple[str, str]] = [
    (r"\bcontract(?:or|ed)?\b|\bfixed[\s-]term\b|\btemporary\b|\binterim\b|\bCDD\b|\bFTC\b",
     "CDD"),
    (r"\bintern(?:ship)?\b|\bstagiaire\b|\boff[\s-]cycle\b|\bsummer\s+analyst\b",
     "Stage"),
    (r"\bworking\s+student\b|\balternance\b|\bapprentice\b",
     "Alternance"),
    (r"\bVIE\b|\bVolontariat\s+International\b",
     "VIE"),
]

# Contract type — Campus TAL
CAMPUS_CONTRACT_MAP: dict[str, str] = {
    "internship":           "Stage",
    "off-cycle internship": "Stage",
    "off cycle internship": "Stage",
    "summer internship":    "Stage",
    "work placement":       "Stage",
    "working student":      "Alternance",
    "apprenticeship":       "Alternance",
    "graduate programme":   "CDI",
    "full-time":            "CDI",
    "permanent":            "CDI",
}

# Entités SF → nom propre
ENTITY_MAP: dict[str, str] = {
    "nomura":    "Nomura",
    "instinet":  "Instinet",
    "greentech": "Nomura Greentech",
    "laser":     "Laser (Nomura)",
}


# ═══════════════════════════════════════════════════════════════
# BASE DE DONNÉES
# ═══════════════════════════════════════════════════════════════

SCHEMA = """
CREATE TABLE IF NOT EXISTS jobs (
    job_id          TEXT PRIMARY KEY,
    source          TEXT NOT NULL,
    candidate_type  TEXT NOT NULL,
    entity          TEXT,
    job_title       TEXT,
    division        TEXT,
    job_family      TEXT,
    job_family_level2 TEXT,
    city            TEXT,
    country         TEXT,
    region          TEXT,
    contract_type   TEXT,
    experience_level TEXT,
    education_level TEXT,
    description     TEXT,
    offer_url       TEXT,
    posted_date     TEXT,
    closing_date    TEXT,
    scraped_at      TEXT
);
CREATE INDEX IF NOT EXISTS idx_region ON jobs(region);
CREATE INDEX IF NOT EXISTS idx_contract ON jobs(contract_type);
CREATE INDEX IF NOT EXISTS idx_entity ON jobs(entity);
"""


def init_db(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.executescript(SCHEMA)
    conn.commit()
    return conn


def get_existing_ids(conn: sqlite3.Connection) -> set[str]:
    return {row[0] for row in conn.execute("SELECT job_id FROM jobs")}


def upsert_job(conn: sqlite3.Connection, job: dict) -> None:
    cols = ", ".join(job.keys())
    placeholders = ", ".join(["?"] * len(job))
    sql = f"INSERT OR REPLACE INTO jobs ({cols}) VALUES ({placeholders})"
    conn.execute(sql, list(job.values()))
    conn.commit()


# ═══════════════════════════════════════════════════════════════
# HTTP UTILITAIRES
# ═══════════════════════════════════════════════════════════════

SESSION = requests.Session()
SESSION.headers.update(HEADERS)


def fetch(url: str, retries: int = MAX_RETRIES) -> Optional[str]:
    for attempt in range(1, retries + 1):
        try:
            r = SESSION.get(url, timeout=REQUEST_TIMEOUT, allow_redirects=True)
            if r.status_code == 200:
                return r.text
            if r.status_code in (403, 404, 410):
                logger.warning(f"HTTP {r.status_code} → {url}")
                return None
            logger.warning(f"HTTP {r.status_code} attempt {attempt}/{retries} → {url}")
        except requests.RequestException as e:
            logger.warning(f"Erreur réseau attempt {attempt}/{retries} : {e} → {url}")
        if attempt < retries:
            time.sleep(2 ** attempt)
    return None


# ═══════════════════════════════════════════════════════════════
# NORMALISATION
# ═══════════════════════════════════════════════════════════════

def normalize_country_region(
    country_code: Optional[str],
    city: Optional[str],
) -> tuple[str, str]:
    """Retourne (pays_fr, région) depuis le code ISO ou le nom de ville."""
    if country_code:
        code = country_code.strip().upper()
        if code in COUNTRY_CODE_MAP:
            return COUNTRY_CODE_MAP[code]
        # Essai : certains sites écrivent le pays en toutes lettres
        mapped = normalize_country(country_code)
        if mapped and mapped != country_code:
            return (mapped, _region_from_country(mapped))
    if city:
        c_lower = city.strip().lower()
        if c_lower in CITY_COUNTRY_MAP:
            return CITY_COUNTRY_MAP[c_lower]
        # Recherche partielle (ex. "New York City" → "new york")
        for key, val in CITY_COUNTRY_MAP.items():
            if key in c_lower or c_lower in key:
                return val
    return ("Non spécifié", "Non spécifié")


def _region_from_country(country_fr: str) -> str:
    eu = {"France", "Allemagne", "Royaume-Uni", "Suisse", "Espagne", "Italie",
          "Pays-Bas", "Belgique", "Suède", "Danemark", "Finlande", "Norvège",
          "Autriche", "Portugal", "Pologne", "Irlande", "Luxembourg", "Grèce",
          "République tchèque", "Hongrie", "Roumanie", "Émirats arabes unis",
          "Arabie Saoudite", "Qatar", "Turquie", "Israël", "Afrique du Sud"}
    am = {"États-Unis", "Canada", "Brésil", "Mexique", "Argentine", "Chili", "Colombie"}
    ap = {"Japon", "Hong Kong", "Singapour", "Australie", "Chine", "Corée du Sud",
          "Inde", "Thaïlande", "Malaisie", "Philippines", "Indonésie", "Taïwan",
          "Vietnam", "Nouvelle-Zélande"}
    if country_fr in eu:
        return "Europe"
    if country_fr in am:
        return "Amérique du Nord"
    if country_fr in ap:
        return "Asie-Pacifique"
    return "Non spécifié"


def extract_level_from_title(title: str) -> str:
    """Extrait le niveau d'expérience depuis le titre du poste."""
    for pattern, level in LEVEL_PATTERNS:
        if re.search(pattern, title, re.IGNORECASE):
            return level
    return "Non spécifié"


def normalize_contract_experienced(title: str, description: str = "") -> str:
    """CDI par défaut pour les experienced hires ; détection CDD/Stage depuis titre+description."""
    combined = f"{title} {description}"
    for pattern, ctype in CONTRACT_EXPERIENCED_PATTERNS:
        if re.search(pattern, combined, re.IGNORECASE):
            return ctype
    return "CDI"


def normalize_campus_contract(raw: str) -> str:
    if not raw:
        return "Stage"
    low = raw.strip().lower()
    for key, val in CAMPUS_CONTRACT_MAP.items():
        if key in low:
            return val
    return raw.strip().title() or "Stage"


def clean_text(text: Optional[str]) -> str:
    if not text:
        return ""
    text = re.sub(r'\s+', ' ', text).strip()
    text = re.sub(r'[ \t]+\n', '\n', text)
    return text


def extract_labeled_field(text: str, label: str) -> Optional[str]:
    """
    Cherche 'Label: Valeur' ou 'Label\\nValeur' dans le texte brut SF.
    """
    patterns = [
        rf'(?:^|\n)\s*{re.escape(label)}\s*[:\-]\s*(.+?)(?:\n|$)',
        rf'{re.escape(label)}\s*[:\-]\s*(.+?)(?:\n|$)',
    ]
    for p in patterns:
        m = re.search(p, text, re.IGNORECASE | re.MULTILINE)
        if m:
            val = m.group(1).strip()
            if val and val.lower() not in ('none', 'n/a', '-', ''):
                return val
    return None


# ═══════════════════════════════════════════════════════════════
# SOURCE 1 — careers.nomura.com (SAP SuccessFactors)
# ═══════════════════════════════════════════════════════════════

def fetch_sitemap() -> list[dict]:
    """
    Télécharge le sitemap et retourne la liste de {url, entity, job_id}.
    """
    logger.info(f"Téléchargement du sitemap : {SITEMAP_URL}")
    xml_text = fetch(SITEMAP_URL)
    if not xml_text:
        logger.error("Impossible de télécharger le sitemap.")
        return []

    # Supprime les namespaces pour simplifier le parsing
    xml_clean = re.sub(r'\s+xmlns[^=]*="[^"]*"', '', xml_text)
    try:
        root = ET.fromstring(xml_clean)
    except ET.ParseError as e:
        logger.error(f"Erreur parsing XML sitemap : {e}")
        return []

    jobs = []
    for loc_el in root.iter('loc'):
        url = (loc_el.text or '').strip()
        # Format : https://careers.nomura.com/[ENTITY]/job/[slug]/[ID]/
        m = re.match(
            r'https://careers\.nomura\.com/([^/]+)/job/([^/]+)/(\d+)/?$',
            url, re.IGNORECASE
        )
        if not m:
            continue
        entity_slug = m.group(1).lower()
        job_id      = m.group(3)
        entity      = ENTITY_MAP.get(entity_slug, m.group(1))
        jobs.append({"url": url, "entity": entity, "job_id": job_id})

    logger.info(f"Sitemap : {len(jobs)} offres trouvées (toutes entités).")
    return jobs


def _extract_joblayout_field(soup: BeautifulSoup, label: str) -> Optional[str]:
    """
    Extrait la valeur d'un champ SF CSB Nomura via le pattern :
      <div class="joblayouttoken …">
        <span class="joblayouttoken-label">Label:</span>
        valeur
      </div>
    """
    for span in soup.select('span.joblayouttoken-label'):
        if label.lower() in span.get_text().lower():
            parent = span.parent
            full = parent.get_text(separator=' ', strip=True)
            # Supprimer le label du début
            value = re.sub(rf'^{re.escape(span.get_text(strip=True))}\s*', '', full).strip()
            # Supprimer éventuels préfixes résiduels "Label :"
            value = re.sub(rf'^{re.escape(label)}\s*:\s*', '', value, flags=re.IGNORECASE).strip()
            if value and value.lower() not in ('none', 'n/a', '-'):
                return value
    return None


def parse_sf_detail(html: str, url: str, entity: str) -> Optional[dict]:
    """
    Parse une page de détail SF careers.nomura.com.
    Structure CSS : div.joblayouttoken / span.joblayouttoken-label / span.jobdescription
    Retourne None si la page est une erreur ou une redirection hors offre.
    """
    soup = BeautifulSoup(html, 'lxml')

    # Détecter les pages d'erreur / redirect (ex. /errorpage/ ou page d'accueil)
    # Les pages de détail d'offre ont toujours au moins un span.joblayouttoken-label
    if not soup.select('span.joblayouttoken-label'):
        logger.debug(f"Page non-offre (pas de joblayouttoken-label) : {url}")
        return None

    full_text = soup.get_text(separator='\n')

    # ── Titre ────────────────────────────────────────────────────
    # SF CSB : le H1 contient "Job Title:Actual Title"
    # div.joblayouttoken avec label "Job Title:" est plus fiable
    title = _extract_joblayout_field(soup, 'Job Title')
    if not title:
        h1 = soup.select_one('h1')
        if h1:
            t_raw = clean_text(h1.get_text())
            title = re.sub(r'^Job\s+Title\s*:\s*', '', t_raw, flags=re.IGNORECASE).strip()
    if not title:
        t = soup.find('title')
        if t:
            title = clean_text(t.get_text()).split('|')[0].split(' - ')[0].strip()
            title = re.sub(r'^Job\s+Title\s*:\s*', '', title).strip()
    if not title:
        logger.warning(f"Titre introuvable : {url}")
        return None

    # ── Métadonnées via joblayouttoken ───────────────────────────
    city         = _extract_joblayout_field(soup, 'City')
    country_code = _extract_joblayout_field(soup, 'Country')
    division     = (_extract_joblayout_field(soup, 'Skill Category')
                    or _extract_joblayout_field(soup, 'Division')
                    or _extract_joblayout_field(soup, 'Department'))
    # Fallback texte brut si joblayouttoken absent
    if not city:
        city = extract_labeled_field(full_text, 'City')
    if not country_code:
        country_code = extract_labeled_field(full_text, 'Country')
    if not division:
        division = extract_labeled_field(full_text, 'Skill Category') or extract_labeled_field(full_text, 'Division')

    # Nettoyage city (parfois contient la région : "London, GB")
    if city and ',' in city:
        city = city.split(',')[0].strip()

    # ── Localisation → pays + région ─────────────────────────────
    country, region = normalize_country_region(country_code, city)

    # ── Description ──────────────────────────────────────────────
    # SF CSB Nomura : <span class="jobdescription">
    desc_el = (
        soup.select_one('span.jobdescription') or
        soup.find('div', class_=re.compile(r'jobdescription|job.description', re.I)) or
        soup.find('div', class_=re.compile(r'description', re.I))
    )
    description = clean_text(desc_el.get_text()) if desc_el else ""
    if not description:
        lines = full_text.split('\n')
        body_lines = [l.strip() for l in lines if len(l.strip()) > 40]
        description = ' '.join(body_lines[:30])

    # ── Niveaux et contrats ───────────────────────────────────────
    experience_level = extract_level_from_title(title)
    contract_type    = normalize_contract_experienced(title, description)
    education_level  = extract_education_level(description, contract_type, title)
    exp_years        = extract_experience_level(description, contract_type, title)

    # ── Classification métier ─────────────────────────────────────
    classify_text  = f"{title} {division or ''}"
    job_family, job_family_l2 = _classify(classify_text)

    # job_id depuis l'URL — format /entity/job/slug/ID/
    m_id = re.search(r'/(\d{5,})/?$', url)
    if not m_id:
        # Essai sur l'avant-dernier segment
        parts = [p for p in url.rstrip('/').split('/') if p]
        last  = parts[-1] if parts else ''
        job_id = last if last.isdigit() else re.sub(r'\W', '_', url[-30:])
    else:
        job_id = m_id.group(1)

    return {
        "job_id":           f"sf_{job_id}",
        "source":           "careers.nomura.com",
        "candidate_type":   "Experienced",
        "entity":           entity,
        "job_title":        title,
        "division":         division or "Non spécifié",
        "job_family":       job_family,
        "job_family_level2": job_family_l2,
        "city":             city or "Non spécifié",
        "country":          country,
        "region":           region,
        "contract_type":    contract_type,
        "experience_level": exp_years or experience_level,
        "education_level":  education_level or "Non spécifié",
        "description":      description[:4000],
        "offer_url":        url,
        "posted_date":      None,
        "closing_date":     None,
        "scraped_at":       datetime.utcnow().isoformat(),
    }


def scrape_sf(conn: sqlite3.Connection) -> int:
    """Scrape toutes les offres SF depuis le sitemap."""
    existing = get_existing_ids(conn)
    sitemap_jobs = fetch_sitemap()
    if not sitemap_jobs:
        return 0

    saved = 0
    total = len(sitemap_jobs)
    for i, entry in enumerate(sitemap_jobs, 1):
        uid = f"sf_{entry['job_id']}"
        if uid in existing:
            logger.debug(f"[{i}/{total}] Déjà en base : {uid}")
            continue

        logger.info(f"[{i}/{total}] Scraping {entry['entity']} #{entry['job_id']} …")
        html = fetch(entry['url'])
        if not html:
            logger.warning(f"Page inaccessible : {entry['url']}")
            time.sleep(REQUEST_DELAY)
            continue

        job = parse_sf_detail(html, entry['url'], entry['entity'])
        if job:
            upsert_job(conn, job)
            existing.add(uid)
            saved += 1
            logger.info(
                f"  ✓ {job['job_title']} | {job['city']}, {job['country']} "
                f"| {job['contract_type']} | {job['experience_level']}"
            )
        else:
            logger.warning(f"  ✗ Parsing échoué : {entry['url']}")

        time.sleep(REQUEST_DELAY)

    logger.info(f"SF : {saved} nouvelles offres enregistrées.")
    return saved


# ═══════════════════════════════════════════════════════════════
# SOURCE 2 — nomuracampus.tal.net (Lumesse TalentLink)
# ═══════════════════════════════════════════════════════════════

def scrape_campus_listing() -> list[dict]:
    """
    Scrape la page listing campus pour extraire tous les liens d'offres.
    Retourne une liste de dicts {slug, title_raw, location_raw,
    region_raw, contract_raw, division_raw, deadline}.
    """
    logger.info(f"Campus listing : {CAMPUS_LIST}")
    html = fetch(CAMPUS_LIST)
    if not html:
        return []

    soup = BeautifulSoup(html, 'lxml')
    jobs = []

    # Chaque offre est dans un <div class="vacancy"> ou <li class="vacancy">
    cards = (
        soup.select('div.vacancy, li.vacancy, .job-listing, .job-card, article.job')
        or soup.select('table.vacancies tr, .vacancies .item')
    )

    if not cards:
        # Fallback : chercher tous les liens /opp/
        links = soup.find_all('a', href=re.compile(r'/opp/\d+'))
        for link in links:
            href = link.get('href', '')
            slug_m = re.search(r'/opp/([\d]+-[^/]+)', href)
            if slug_m:
                jobs.append({
                    'slug': slug_m.group(1),
                    'title_raw': clean_text(link.get_text()),
                    'location_raw': '',
                    'region_raw': '',
                    'contract_raw': '',
                    'division_raw': '',
                    'deadline': '',
                })
    else:
        for card in cards:
            link = card.find('a', href=re.compile(r'/opp/\d+'))
            if not link:
                continue
            href = link.get('href', '')
            slug_m = re.search(r'/opp/([\d]+-[^/]+)', href)
            if not slug_m:
                continue

            def card_text(cls_pattern: str) -> str:
                el = card.find(class_=re.compile(cls_pattern, re.I))
                return clean_text(el.get_text()) if el else ''

            jobs.append({
                'slug':         slug_m.group(1),
                'title_raw':    clean_text(link.get_text()),
                'location_raw': card_text(r'location|city|lieu'),
                'region_raw':   card_text(r'region'),
                'contract_raw': card_text(r'contract|type'),
                'division_raw': card_text(r'division|department|function'),
                'deadline':     card_text(r'deadline|closing|date'),
            })

    logger.info(f"Campus listing : {len(jobs)} offres détectées.")
    return jobs


def _extract_talnet_field(soup: BeautifulSoup, label: str) -> Optional[str]:
    """
    TAL.NET structure :
      <div class="form-group …">
        <label …><span class="hform_lbl_text">Location</span></label>
        <div class="col-sm-9 form_value …">Zurich</div>
      </div>
    """
    for span in soup.find_all('span', class_='hform_lbl_text'):
        if label.lower() in span.get_text(strip=True).lower():
            row = span.find_parent(class_='form-group')
            if row:
                val_el = row.select_one('.col-sm-9.form_value, .col-sm-9.hform_value_view, div.col-sm-9')
                if val_el:
                    val = clean_text(val_el.get_text())
                    if val and val.lower() not in ('none', 'n/a', '-', ''):
                        return val
    return None


def parse_campus_detail(html: str, slug: str, listing_meta: dict) -> Optional[dict]:
    """
    Parse une page de détail campus TAL.NET.
    Combine les données du listing (région, deadline) + le détail (ville, pays, description).
    """
    soup = BeautifulSoup(html, 'lxml')
    full_text = soup.get_text(separator='\n')

    # ── Titre ────────────────────────────────────────────────────
    title = listing_meta.get('title_raw', '')
    for sel in ['h1.job-title', 'h1[class*="title"]', 'h1']:
        el = soup.select_one(sel)
        if el:
            title = clean_text(el.get_text())
            break

    # ── Localisation ─────────────────────────────────────────────
    # TAL.NET utilise des champs de formulaire : hform_lbl_text + col-sm-9.form_value
    city    = (_extract_talnet_field(soup, 'Location')
               or _extract_talnet_field(soup, 'City')
               or extract_labeled_field(full_text, 'Location')
               or extract_labeled_field(full_text, 'City')
               or listing_meta.get('location_raw', ''))
    country_raw = (_extract_talnet_field(soup, 'Country')
                   or extract_labeled_field(full_text, 'Country') or '')

    # Normaliser la région depuis le listing OU depuis le champ "Region" de la page TAL.NET
    region_raw = (
        (_extract_talnet_field(soup, 'Region') or listing_meta.get('region_raw', ''))
    ).lower()
    if 'emea' in region_raw or 'europe' in region_raw:
        region_hint = 'Europe'
    elif 'america' in region_raw:
        region_hint = 'Amérique du Nord'
    elif 'asia' in region_raw or 'aej' in region_raw or 'apac' in region_raw:
        region_hint = 'Asie-Pacifique'
    else:
        region_hint = None

    # Essayer de déduire depuis la ville ou le titre
    city_lower = (city or title).lower()
    country, region = normalize_country_region(country_raw or None, city_lower)
    if region == "Non spécifié" and region_hint:
        region = region_hint
    if country == "Non spécifié" and city:
        country, region = normalize_country_region(None, city)

    # Fallback ville depuis le titre campus
    # Format 1 : "2026 - Category – City" (em dash avant ville)
    # Format 2 : "2026 - Category - Type - City" (dernier segment sans mots-clés de type)
    if (not city or city == "Non spécifié") and title:
        city_candidate = None
        if ' – ' in title or ' — ' in title:
            # Em dash ou en dash → ce qui suit est la ville
            city_candidate = re.split(r' [–—] ', title)[-1].strip()
            # Nettoyer les suffixes de session : "(July start)", "(September Start)" etc.
            city_candidate = re.sub(r'\s*\([^)]*start[^)]*\)', '', city_candidate, flags=re.I).strip()
        if not city_candidate:
            # Hyphen dash : prendre la dernière partie si ça ressemble à une ville
            parts = [p.strip() for p in title.split(' - ')]
            last = parts[-1] if parts else ''
            # Nettoyer les suffixes de session avant de tester si c'est une ville
            last_clean = re.sub(r'\s*\([^)]*(?:start|intake|cohort)[^)]*\)', '', last, flags=re.I).strip()
            if last_clean and not re.search(
                r'(programme|program|internship|analyst|intern|placement|student)',
                last_clean, re.I
            ):
                city_candidate = last_clean
        if city_candidate:
            city = city_candidate
            c2, r2 = normalize_country_region(None, city_candidate.lower())
            if country == "Non spécifié":
                country = c2
            if region == "Non spécifié":
                region = r2

    # ── Division ─────────────────────────────────────────────────
    division = (
        _extract_talnet_field(soup, 'Business Area')
        or _extract_talnet_field(soup, 'Division')
        or _extract_talnet_field(soup, 'Department')
        or extract_labeled_field(full_text, 'Division')
        or extract_labeled_field(full_text, 'Business Area')
        or extract_labeled_field(full_text, 'Department')
        or listing_meta.get('division_raw', '')
        or _infer_division_from_title(title)
    )

    # ── Contrat ───────────────────────────────────────────────────
    contract_raw = (
        _extract_talnet_field(soup, 'Contract Type')
        or _extract_talnet_field(soup, 'Type')
        or extract_labeled_field(full_text, 'Contract Type')
        or extract_labeled_field(full_text, 'Type')
        or listing_meta.get('contract_raw', '')
        or _infer_contract_from_title(title)
    )
    contract_type = normalize_campus_contract(contract_raw)
    # Override title-based : "Working Student" → Alternance (même si la source dit "Work Placement")
    if re.search(r'\bworking\s+student\b', title, re.IGNORECASE):
        contract_type = "Alternance"

    # ── Description ──────────────────────────────────────────────
    desc_el = (
        soup.find('div', class_=re.compile(r'description|job.body|content', re.I)) or
        soup.find('section', class_=re.compile(r'description', re.I))
    )
    description = clean_text(desc_el.get_text()) if desc_el else ""
    if not description:
        lines = [l.strip() for l in full_text.split('\n') if len(l.strip()) > 50]
        description = ' '.join(lines[:30])

    # ── Niveaux ───────────────────────────────────────────────────
    experience_level = extract_level_from_title(title) if 'intern' not in title.lower() else "Intern / Stage"
    education_level  = extract_education_level(description, contract_type, title)
    exp_years        = extract_experience_level(description, contract_type, title)

    # ── Classification ────────────────────────────────────────────
    classify_text = f"{title} {division or ''}"
    job_family, job_family_l2 = _classify(classify_text)

    # ── Deadline ─────────────────────────────────────────────────
    deadline = listing_meta.get('deadline', '')
    if not deadline:
        deadline = extract_labeled_field(full_text, 'Closing Date') or ''

    # ── ID depuis slug ────────────────────────────────────────────
    opp_id_m = re.match(r'^(\d+)', slug)
    opp_id   = opp_id_m.group(1) if opp_id_m else slug[:20]

    return {
        "job_id":           f"campus_{opp_id}",
        "source":           "nomuracampus.tal.net",
        "candidate_type":   "Campus",
        "entity":           "Nomura",
        "job_title":        title or "Non spécifié",
        "division":         division or "Non spécifié",
        "job_family":       job_family,
        "job_family_level2": job_family_l2,
        "city":             city or "Non spécifié",
        "country":          country,
        "region":           region,
        "contract_type":    contract_type,
        "experience_level": exp_years or experience_level,
        "education_level":  education_level or "Non spécifié",
        "description":      description[:4000],
        "offer_url":        CAMPUS_DETAIL.format(slug=slug),
        "posted_date":      None,
        "closing_date":     deadline or None,
        "scraped_at":       datetime.utcnow().isoformat(),
    }


def _infer_division_from_title(title: str) -> str:
    """Infère la division depuis les mots-clés du titre."""
    mapping = [
        (r'\bglobal\s+markets?\b|\bGM\b|\bfixed\s+income\b|\bequit(?:y|ies)\b|\brates?\b|\bfx\b|\bforex\b|\bderivatives?\b|\bsales?\b|\btrading\b|\bstructured\b',
         "Global Markets"),
        (r'\binvestment\s+banking?\b|\bIBD\b|\bm&a\b|\bmergers?\b|\bacquisitions?\b|\bdcm\b|\becm\b|\bcapital\s+markets?\b',
         "Investment Banking"),
        (r'\basset\s+management\b|\bportfolio\b|\binvestment\s+management\b',
         "Asset Management"),
        (r'\brisk\s+management\b|\bcredit\s+risk\b|\bmarket\s+risk\b|\boperational\s+risk\b',
         "Risk Management"),
        (r'\bfinance\b|\baccounting\b|\bfinancial\s+report\b|\bcontrol(?:ler|ling)?\b',
         "Finance"),
        (r'\btechnology\b|\btech\b|\bsoftware\b|\bIT\b|\binfrastructure\b|\bdata\b|\bengineering\b',
         "Technology"),
        (r'\boperations?\b|\bops\b|\bsettlement\b|\bclearning\b|\bpost.trade\b',
         "Operations"),
        (r'\bcompliance\b|\bregulatory\b|\bAML\b|\bKYC\b',
         "Compliance"),
        (r'\blegal\b|\bcounsel\b',
         "Legal"),
        (r'\bhuman\s+resources?\b|\bHR\b|\btalent\b|\brecruit',
         "Human Resources"),
        (r'\baudit\b',
         "Internal Audit"),
    ]
    for pattern, division in mapping:
        if re.search(pattern, title, re.IGNORECASE):
            return division
    return "Non spécifié"


def _infer_contract_from_title(title: str) -> str:
    """Infère le type de contrat depuis le titre."""
    t = title.lower()
    if any(x in t for x in ['off-cycle', 'off cycle', 'internship', 'intern', 'summer analyst', 'work placement']):
        return 'internship'
    if any(x in t for x in ['working student', 'alternance', 'apprenti']):
        return 'working student'
    if 'graduate programme' in t or 'analyst program' in t:
        return 'graduate programme'
    return 'internship'  # campus par défaut


def scrape_campus(conn: sqlite3.Connection) -> int:
    """Scrape toutes les offres campus TAL.NET."""
    existing = get_existing_ids(conn)
    listings = scrape_campus_listing()
    if not listings:
        logger.warning("Aucune offre campus détectée.")
        return 0

    saved = 0
    total = len(listings)
    for i, meta in enumerate(listings, 1):
        opp_id_m = re.match(r'^(\d+)', meta['slug'])
        opp_id   = opp_id_m.group(1) if opp_id_m else meta['slug'][:20]
        uid      = f"campus_{opp_id}"

        if uid in existing:
            logger.debug(f"[{i}/{total}] Campus déjà en base : {uid}")
            continue

        detail_url = CAMPUS_DETAIL.format(slug=meta['slug'])
        logger.info(f"[{i}/{total}] Campus scraping : {meta['title_raw'][:60]}")
        html = fetch(detail_url)
        if not html:
            logger.warning(f"Campus page inaccessible : {detail_url}")
            time.sleep(REQUEST_DELAY)
            continue

        job = parse_campus_detail(html, meta['slug'], meta)
        if job:
            upsert_job(conn, job)
            existing.add(uid)
            saved += 1
            logger.info(
                f"  ✓ {job['job_title'][:60]} | {job['city']}, {job['country']} "
                f"| {job['contract_type']} | {job['region']}"
            )
        else:
            logger.warning(f"  ✗ Campus parsing échoué : {detail_url}")

        time.sleep(REQUEST_DELAY)

    logger.info(f"Campus : {saved} nouvelles offres enregistrées.")
    return saved


# ═══════════════════════════════════════════════════════════════
# CLASSIFICATION MÉTIER
# ═══════════════════════════════════════════════════════════════

def _classify(text: str) -> tuple[str, str]:
    """Retourne (job_family, job_family_level2) depuis classify_job_family."""
    try:
        result = classify_job_family(text)
        if isinstance(result, tuple):
            return result[0] or "Non spécifié", result[1] or ""
        return str(result) or "Non spécifié", ""
    except Exception:
        return "Non spécifié", ""


# ═══════════════════════════════════════════════════════════════
# EXPORT CSV
# ═══════════════════════════════════════════════════════════════

def export_csv(conn: sqlite3.Connection, path: Path) -> None:
    import csv
    rows = conn.execute("SELECT * FROM jobs ORDER BY region, country, job_title").fetchall()
    cols = [d[0] for d in conn.execute("SELECT * FROM jobs LIMIT 0").description]
    with open(path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(cols)
        writer.writerows(rows)
    logger.info(f"CSV exporté : {path} ({len(rows)} lignes)")


# ═══════════════════════════════════════════════════════════════
# STATISTIQUES
# ═══════════════════════════════════════════════════════════════

def print_stats(conn: sqlite3.Connection) -> None:
    total = conn.execute("SELECT COUNT(*) FROM jobs").fetchone()[0]
    logger.info(f"\n{'='*60}")
    logger.info(f"TOTAL : {total} offres Nomura")

    logger.info("\n── Par région ──")
    for row in conn.execute(
        "SELECT region, COUNT(*) n FROM jobs GROUP BY region ORDER BY n DESC"
    ):
        logger.info(f"  {row[0]:<25} {row[1]:>4}")

    logger.info("\n── Par type de candidat ──")
    for row in conn.execute(
        "SELECT candidate_type, COUNT(*) n FROM jobs GROUP BY candidate_type ORDER BY n DESC"
    ):
        logger.info(f"  {row[0]:<25} {row[1]:>4}")

    logger.info("\n── Par contrat ──")
    for row in conn.execute(
        "SELECT contract_type, COUNT(*) n FROM jobs GROUP BY contract_type ORDER BY n DESC"
    ):
        logger.info(f"  {row[0]:<25} {row[1]:>4}")

    logger.info("\n── Par entité ──")
    for row in conn.execute(
        "SELECT entity, COUNT(*) n FROM jobs GROUP BY entity ORDER BY n DESC"
    ):
        logger.info(f"  {row[0]:<25} {row[1]:>4}")

    logger.info("\n── Données manquantes ──")
    for col in ('city', 'country', 'region', 'contract_type', 'experience_level', 'division'):
        n = conn.execute(
            f"SELECT COUNT(*) FROM jobs WHERE {col} IS NULL OR {col} = 'Non spécifié'"
        ).fetchone()[0]
        if n:
            logger.info(f"  ⚠️  {col:<22} : {n} / {total} non spécifiés")

    logger.info('='*60)


# ═══════════════════════════════════════════════════════════════
# POINT D'ENTRÉE
# ═══════════════════════════════════════════════════════════════

def main() -> None:
    import argparse
    parser = argparse.ArgumentParser(description="Nomura Job Scraper")
    parser.add_argument('--only-campus',     action='store_true', help='Scrape campus uniquement')
    parser.add_argument('--only-sf',         action='store_true', help='Scrape SF (experienced) uniquement')
    parser.add_argument('--export-csv',      action='store_true', help='Exporter CSV après scraping')
    parser.add_argument('--stats-only',      action='store_true', help='Afficher stats sans scraper')
    parser.add_argument('--db', default=str(DB_PATH), help='Chemin SQLite')
    args = parser.parse_args()

    db_path = Path(args.db)
    conn = init_db(db_path)
    logger.info(f"Base de données : {db_path}")

    if args.stats_only:
        print_stats(conn)
        return

    sf_count     = 0
    campus_count = 0

    if not args.only_campus:
        sf_count = scrape_sf(conn)

    if not args.only_sf:
        campus_count = scrape_campus(conn)

    logger.info(f"\n✅ Scraping terminé : {sf_count} SF + {campus_count} campus = {sf_count + campus_count} nouvelles offres")
    print_stats(conn)

    if args.export_csv:
        csv_path = db_path.with_suffix('.csv')
        export_csv(conn, csv_path)

    conn.close()


if __name__ == '__main__':
    main()
