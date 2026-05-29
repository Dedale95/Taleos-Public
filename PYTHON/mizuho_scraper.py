#!/usr/bin/env python3
"""
Mizuho scraper — trois sources :
  1. Americas  : Workday API  mizuho.wd1.myworkdayjobs.com/mizuhoamericas       (~89 offres)
  2. APAC      : Workday API  mizuhogroup.wd102.myworkdayjobs.com/External        (~5 offres)
  3. EMEA      : HTML scrape  careers.mizuhoemea.com                             (~13 offres)

Usage :
  python3 mizuho_scraper.py            # scrape + insert DB
  python3 mizuho_scraper.py --stats-only
  python3 mizuho_scraper.py --export-csv
"""

import argparse
import csv
import html as html_lib
import logging
import re
import sqlite3
import sys
import time
from datetime import date, datetime
from pathlib import Path
from urllib.parse import unquote

import requests

# ── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ── Chemins ──────────────────────────────────────────────────────────────────
SCRIPT_DIR = Path(__file__).parent
DB_PATH    = SCRIPT_DIR / "mizuho_jobs.db"

# ── Workday endpoints ─────────────────────────────────────────────────────────
SOURCES_WD = [
    {
        "id":        "americas",
        "tenant":    "mizuho",
        "site":      "mizuhoamericas",
        "host":      "mizuho.wd1.myworkdayjobs.com",
        "region":    "Amérique du Nord",
    },
    {
        "id":        "apac",
        "tenant":    "mizuhogroup",
        "site":      "External",
        "host":      "mizuhogroup.wd102.myworkdayjobs.com",
        "region":    "Asie-Pacifique",   # fallback si alpha2 inconnu
    },
]

# EMEA — careers.mizuhoemea.com (SAP SuccessFactors hosted)
EMEA_SEARCH_URL = (
    "https://careers.mizuhoemea.com/search-results"
    "?AllFields=&Country=&City=&resultNumber=200&startRow=0"
)
EMEA_BASE_URL = "https://careers.mizuhoemea.com"

PAGE_SIZE     = 20          # limite Workday sûre
REQUEST_DELAY = 0.6         # secondes entre requêtes

# ── Correspondances pays ──────────────────────────────────────────────────────
COUNTRY_CODE_MAP: dict[str, tuple[str, str]] = {
    # Amérique du Nord
    "US": ("États-Unis",       "Amérique du Nord"),
    "CA": ("Canada",           "Amérique du Nord"),
    "MX": ("Mexique",          "Amérique du Nord"),
    # Europe
    "GB": ("Royaume-Uni",      "Europe"),
    "DE": ("Allemagne",        "Europe"),
    "FR": ("France",           "Europe"),
    "NL": ("Pays-Bas",         "Europe"),
    "ES": ("Espagne",          "Europe"),
    "IT": ("Italie",           "Europe"),
    "CH": ("Suisse",           "Europe"),
    "LU": ("Luxembourg",       "Europe"),
    "IE": ("Irlande",          "Europe"),
    "PL": ("Pologne",          "Europe"),
    "BE": ("Belgique",         "Europe"),
    "SE": ("Suède",            "Europe"),
    "DK": ("Danemark",         "Europe"),
    # Moyen-Orient / Afrique (EMEA élargi)
    "AE": ("Émirats arabes unis", "Europe"),
    "SA": ("Arabie saoudite",  "Europe"),
    "QA": ("Qatar",            "Europe"),
    "ZA": ("Afrique du Sud",   "Europe"),
    # Asie-Pacifique
    "JP": ("Japon",            "Asie-Pacifique"),
    "HK": ("Hong Kong",        "Asie-Pacifique"),
    "SG": ("Singapour",        "Asie-Pacifique"),
    "CN": ("Chine",            "Asie-Pacifique"),
    "IN": ("Inde",             "Asie-Pacifique"),
    "AU": ("Australie",        "Asie-Pacifique"),
    "KR": ("Corée du Sud",     "Asie-Pacifique"),
    "TW": ("Taïwan",           "Asie-Pacifique"),
    "TH": ("Thaïlande",        "Asie-Pacifique"),
    "MY": ("Malaisie",         "Asie-Pacifique"),
    "ID": ("Indonésie",        "Asie-Pacifique"),
    "PH": ("Philippines",      "Asie-Pacifique"),
    "VN": ("Vietnam",          "Asie-Pacifique"),
}

# Noms pays Workday anglais → alpha2
COUNTRY_EN_MAP: dict[str, str] = {
    "united states of america": "US",
    "united states":            "US",
    "canada":                   "CA",
    "united kingdom":           "GB",
    "germany":                  "DE",
    "france":                   "FR",
    "netherlands":              "NL",
    "spain":                    "ES",
    "italy":                    "IT",
    "switzerland":              "CH",
    "luxembourg":               "LU",
    "ireland":                  "IE",
    "poland":                   "PL",
    "belgium":                  "BE",
    "sweden":                   "SE",
    "denmark":                  "DK",
    "united arab emirates":     "AE",
    "saudi arabia":             "SA",
    "qatar":                    "QA",
    "south africa":             "ZA",
    "japan":                    "JP",
    "hong kong":                "HK",
    "hong kong sar":            "HK",
    "singapore":                "SG",
    "china":                    "CN",
    "india":                    "IN",
    "australia":                "AU",
    "korea, republic of":       "KR",
    "south korea":              "KR",
    "taiwan":                   "TW",
    "thailand":                 "TH",
    "malaysia":                 "MY",
    "indonesia":                "ID",
    "philippines":              "PH",
    "vietnam":                  "VN",
}

# Villes → alpha2 (fallback quand alpha2 absent)
CITY_COUNTRY_MAP: dict[str, str] = {
    "new york":   "US", "nyc":        "US", "chicago":   "US",
    "houston":    "US", "los angeles":"US", "san francisco": "US",
    "boston":     "US", "washington": "US", "jersey city": "US",
    "toronto":    "CA", "montreal":   "CA", "vancouver":  "CA",
    "london":     "GB", "edinburgh":  "GB", "manchester": "GB",
    "paris":      "FR", "frankfurt":  "DE", "munich":     "DE",
    "amsterdam":  "NL", "madrid":     "ES", "milan":      "IT",
    "zurich":     "CH", "geneva":     "CH", "luxembourg": "LU",
    "dublin":     "IE", "warsaw":     "PL", "brussels":   "BE",
    "dubai":      "AE", "abu dhabi":  "AE", "riyadh":     "SA",
    "doha":       "QA", "johannesburg": "ZA",
    "tokyo":      "JP", "osaka":      "JP",
    "hong kong":  "HK", "hk":         "HK",
    "singapore":  "SG",
    "beijing":    "CN", "shanghai":   "CN", "guangzhou":  "CN",
    "mumbai":     "IN", "bangalore":  "IN", "hyderabad":  "IN",
    "sydney":     "AU", "melbourne":  "AU",
    "seoul":      "KR", "taipei":     "TW", "bangkok":    "TH",
    "kuala lumpur": "MY", "jakarta":  "ID", "manila":     "PH",
    "ho chi minh": "VN", "hanoi":     "VN",
}

# ── Famille métier ─────────────────────────────────────────────────────────────
FAMILY_MAP: dict[str, str] = {
    # Marchés
    "sales & trading":              "Marchés financiers / Sales & Trading",
    "sales and trading":            "Marchés financiers / Sales & Trading",
    "markets":                      "Marchés financiers / Sales & Trading",
    "trading":                      "Marchés financiers / Sales & Trading",
    "equity":                       "Marchés financiers / Sales & Trading",
    "equities":                     "Marchés financiers / Sales & Trading",
    "fixed income":                 "Marchés financiers / Sales & Trading",
    "derivatives":                  "Marchés financiers / Sales & Trading",
    "structured products":          "Marchés financiers / Sales & Trading",
    "research":                     "Marchés financiers / Sales & Trading",
    "securities":                   "Marchés financiers / Sales & Trading",
    "capital markets":              "Marchés financiers / Sales & Trading",
    "clo":                          "Marchés financiers / Sales & Trading",
    # Investment Banking
    "investment banking":           "Financement et Investissement",
    "corporate banking":            "Financement et Investissement",
    "corporate finance":            "Financement et Investissement",
    "m&a":                          "Financement et Investissement",
    "leveraged finance":            "Financement et Investissement",
    "loan":                         "Financement et Investissement",
    "syndication":                  "Financement et Investissement",
    "infrastructure":               "Financement et Investissement",
    "project finance":              "Financement et Investissement",
    "debt capital":                 "Financement et Investissement",
    # IT / Tech
    "technology":                   "IT, Digital et Data",
    "information technology":       "IT, Digital et Data",
    "software":                     "IT, Digital et Data",
    "data":                         "IT, Digital et Data",
    "digital":                      "IT, Digital et Data",
    "cybersecurity":                "IT, Digital et Data",
    "infrastructure technology":    "IT, Digital et Data",
    "murex":                        "IT, Digital et Data",
    "developer":                    "IT, Digital et Data",
    "java":                         "IT, Digital et Data",
    "engineering":                  "IT, Digital et Data",
    # Risk / Compliance
    "risk":                         "Gestion des risques",
    "credit risk":                  "Gestion des risques",
    "market risk":                  "Gestion des risques",
    "compliance":                   "Conformité / Sécurité financière",
    "financial crime":              "Conformité / Sécurité financière",
    "kyc":                          "Conformité / Sécurité financière",
    "aml":                          "Conformité / Sécurité financière",
    "audit":                        "Audit / Contrôle interne",
    "internal audit":               "Audit / Contrôle interne",
    "legal":                        "Juridique / Conformité",
    # Finance / Ops
    "finance":                      "Finance / Comptabilité",
    "accounting":                   "Finance / Comptabilité",
    "tax":                          "Finance / Comptabilité",
    "treasury":                     "Finance / Comptabilité",
    "operations":                   "Opérations / Back Office",
    "ops":                          "Opérations / Back Office",
    "back office":                  "Opérations / Back Office",
    "settlements":                  "Opérations / Back Office",
    "collateral":                   "Opérations / Back Office",
    "loan agency":                  "Opérations / Back Office",
    "client services":              "Opérations / Back Office",
    # Support
    "human resources":              "Ressources Humaines",
    "hr":                           "Ressources Humaines",
    "recruiting":                   "Ressources Humaines",
    "talent":                       "Ressources Humaines",
    "communications":               "Communication / Marketing",
    "marketing":                    "Communication / Marketing",
    "strategy":                     "Stratégie / Management",
    "management reporting":         "Finance / Comptabilité",
    "planning":                     "Stratégie / Management",
}

# ── Niveau d'expérience ────────────────────────────────────────────────────────
LEVEL_PATTERNS: list[tuple[re.Pattern, str]] = [
    # C-Suite / MD / ED (le plus haut)
    (re.compile(r"\bManaging\s+Director\b|\bMD\b",                          re.I), "11 ans et plus"),
    (re.compile(r"\bExecutive\s+Director\b|\bED\b",                         re.I), "11 ans et plus"),
    (re.compile(r"\bChief\s+(?:\w+\s+)?Officer\b|\bCEO\b|\bCOO\b|\bCFO\b", re.I), "11 ans et plus"),
    (re.compile(r"\bHead\s+of\b|\bGlobal\s+Head\b|\bRegional\s+Head\b",    re.I), "6 - 10 ans"),
    (re.compile(r"\bDirector\b",                                             re.I), "6 - 10 ans"),
    (re.compile(r"\bVice\s+President\b|\bVP\b",                             re.I), "6 - 10 ans"),
    # AVP / Senior
    (re.compile(r"\bAssistant\s+Vice\s+President\b|\bAVP\b",                re.I), "3 - 5 ans"),
    (re.compile(r"\bSenior\b|\bSr\b\.?",                                    re.I), "3 - 5 ans"),
    (re.compile(r"\bLead\b",                                                 re.I), "3 - 5 ans"),
    (re.compile(r"\bManager\b",                                              re.I), "3 - 5 ans"),
    (re.compile(r"\bPrincipal\b",                                            re.I), "3 - 5 ans"),
    (re.compile(r"\bCounsel\b|\bAttorney\b|\bLawyer\b",                     re.I), "3 - 5 ans"),
    (re.compile(r"\bArchitect\b|\bSpecialist\b|\bExpert\b",                  re.I), "3 - 5 ans"),
    (re.compile(r"\bEngineer\b",                                             re.I), "3 - 5 ans"),
    # Associate / Officer / Auditor / standard
    (re.compile(r"\bAssociate\b",                                            re.I), "1 - 2 ans"),
    (re.compile(r"\bOfficer\b",                                              re.I), "1 - 2 ans"),
    (re.compile(r"\bAuditor\b",                                              re.I), "1 - 2 ans"),
    # Analyst / junior
    (re.compile(r"\bAnalyst\b",                                              re.I), "0 - 2 ans"),
    (re.compile(r"\bGraduate\b|\bGrad\b",                                   re.I), "0 - 2 ans"),
    (re.compile(r"\bIntern(?:ship)?\b|\bSummer\s+Analyst\b",                re.I), "0 - 2 ans"),
    (re.compile(r"\bEntry[\s-]Level\b|\bJunior\b",                          re.I), "0 - 2 ans"),
]

CAMPUS_PATTERNS = re.compile(
    r"\bIntern(?:ship)?\b|\bSummer\s+Analyst\b|\bGraduate\s+(?:Analyst|Programme|Program|Recruit)\b"
    r"|\bOff[\s-]Cycle\b|\bNew\s+Graduate\b|\bEntry[\s-]Level\b|\bApprentice\b",
    re.IGNORECASE,
)

# ── Utilitaires ───────────────────────────────────────────────────────────────
def clean_html(raw: str) -> str:
    """Supprime les balises HTML et décode les entités."""
    s = re.sub(r"<[^>]+>", " ", raw or "")
    s = html_lib.unescape(s)
    return re.sub(r"\s+", " ", s).strip()


def extract_city(location_text: str) -> str:
    """Extrait la ville depuis 'New York, NY' / 'NYC (1285)' / 'London (Lon), GB'."""
    if not location_text:
        return ""
    loc = clean_html(location_text).strip()
    # Supprimer le code pays ISO en fin "..., GB"
    loc = re.sub(r",?\s*[A-Z]{2}$", "", loc).strip()
    # Supprimer les suffixes entre parenthèses : "NYC (1285)" → "NYC"
    loc = re.sub(r"\s*\([^)]*\)", "", loc).strip()
    # Nettoyer état US "New York, NY" → "New York"
    loc = re.sub(r",\s*[A-Z]{2}$", "", loc).strip()
    return loc


def resolve_country(alpha2: str | None, country_name: str | None, city: str) -> tuple[str, str]:
    """Retourne (pays_fr, région)."""
    if alpha2 and alpha2.upper() in COUNTRY_CODE_MAP:
        return COUNTRY_CODE_MAP[alpha2.upper()]
    if country_name:
        key = country_name.lower().strip()
        a2 = COUNTRY_EN_MAP.get(key)
        if a2 and a2 in COUNTRY_CODE_MAP:
            return COUNTRY_CODE_MAP[a2]
    if city:
        key = city.lower().strip()
        a2 = CITY_COUNTRY_MAP.get(key)
        if a2 and a2 in COUNTRY_CODE_MAP:
            return COUNTRY_CODE_MAP[a2]
    return "Non spécifié", "Non spécifié"


def classify_family(title: str, description: str = "") -> str:
    """Détermine la famille métier depuis le titre (priorité) + description.
    On cherche d'abord dans le titre seul (plus fiable), puis dans le texte combiné."""
    title_l = title.lower()
    combined = f"{title} {description}".lower()

    # Règles titre-only par ordre de spécificité décroissante
    title_rules: list[tuple[str, str]] = [
        # Audit (avant compliance pour éviter faux positifs)
        ("audit",               "Audit / Contrôle interne"),
        # Conformité / Financial crime (avant "sales & trading" pour "compliance")
        ("financial crime",     "Conformité / Sécurité financière"),
        ("kyc",                 "Conformité / Sécurité financière"),
        ("aml",                 "Conformité / Sécurité financière"),
        ("transaction monitoring", "Conformité / Sécurité financière"),
        ("compliance",          "Conformité / Sécurité financière"),
        # Juridique
        ("attorney",            "Juridique / Conformité"),
        ("counsel",             "Juridique / Conformité"),
        ("lawyer",              "Juridique / Conformité"),
        ("legal",               "Juridique / Conformité"),
        # IT (avant markets pour "market data", "site reliability"…)
        ("site reliability",    "IT, Digital et Data"),
        ("developer",           "IT, Digital et Data"),
        ("architect",           "IT, Digital et Data"),
        ("java",                "IT, Digital et Data"),
        ("python",              "IT, Digital et Data"),
        ("sql",                 "IT, Digital et Data"),
        ("dba",                 "IT, Digital et Data"),
        ("servicenow",          "IT, Digital et Data"),
        ("onbase",              "IT, Digital et Data"),
        ("cybersecurity",       "IT, Digital et Data"),
        ("cyber defense",       "IT, Digital et Data"),
        ("privileged access",   "IT, Digital et Data"),
        ("infrastructure technology", "IT, Digital et Data"),
        ("market data",         "IT, Digital et Data"),        # market DATA ≠ marchés
        ("data integration",    "IT, Digital et Data"),
        ("data steward",        "IT, Digital et Data"),
        ("crm",                 "IT, Digital et Data"),
        ("technology",          "IT, Digital et Data"),
        ("information technology", "IT, Digital et Data"),
        ("digital",             "IT, Digital et Data"),
        ("software",            "IT, Digital et Data"),
        # "it " exclut avec regex (substring "it " matche "credit " sinon)
        # Risk
        ("credit risk",         "Gestion des risques"),
        ("market risk",         "Gestion des risques"),
        ("irrbb",               "Gestion des risques"),
        ("alm",                 "Gestion des risques"),
        ("operational resilience", "Gestion des risques"),
        ("risk",                "Gestion des risques"),
        # Finance / Compta
        ("accounting",          "Finance / Comptabilité"),
        ("payroll",             "Finance / Comptabilité"),
        ("tax",                 "Finance / Comptabilité"),
        ("management reporting","Finance / Comptabilité"),
        ("regulatory reporting","Finance / Comptabilité"),
        ("regulatory change",   "Finance / Comptabilité"),
        ("finance",             "Finance / Comptabilité"),
        ("treasury",            "Finance / Comptabilité"),
        # Opérations / Back-office
        ("operations",          "Opérations / Back Office"),
        ("settlements",         "Opérations / Back Office"),
        ("collateral",          "Opérations / Back Office"),
        ("loan agency",         "Opérations / Back Office"),
        ("standby letter",      "Opérations / Back Office"),
        ("trade finance",       "Opérations / Back Office"),
        ("transaction banking", "Opérations / Back Office"),
        ("cash management",     "Opérations / Back Office"),
        # RH
        ("human resources",     "Ressources Humaines"),
        ("talent",              "Ressources Humaines"),
        ("payroll",             "Ressources Humaines"),       # second hit — déjà Finance mais ok
        ("recruiting",          "Ressources Humaines"),
        ("events manager",      "Ressources Humaines"),
        # Communication / Marketing
        ("communications",      "Communication / Marketing"),
        ("marketing",           "Communication / Marketing"),
        # Stratégie
        ("strategy",            "Stratégie / Management"),
        ("planning",            "Stratégie / Management"),
        # Coverage / IB (avant marchés pour "MEA Coverage")
        ("coverage",            "Financement et Investissement"),
        ("mea coverage",        "Financement et Investissement"),
        # Réconciliations / Nostro
        ("nostro",              "Finance / Comptabilité"),
        ("reconciliation",      "Finance / Comptabilité"),
        # Marchés (en dernier pour éviter faux positifs sur "market data")
        ("sales & trading",     "Marchés financiers / Sales & Trading"),
        ("structured products", "Marchés financiers / Sales & Trading"),
        ("derivatives",         "Marchés financiers / Sales & Trading"),
        ("fixed income",        "Marchés financiers / Sales & Trading"),
        ("equity",              "Marchés financiers / Sales & Trading"),
        ("equities",            "Marchés financiers / Sales & Trading"),
        ("clo",                 "Marchés financiers / Sales & Trading"),
        ("capital markets",     "Marchés financiers / Sales & Trading"),
        ("securities",          "Marchés financiers / Sales & Trading"),
        ("research",            "Marchés financiers / Sales & Trading"),
        ("trading",             "Marchés financiers / Sales & Trading"),
        ("sales",               "Marchés financiers / Sales & Trading"),
        ("markets",             "Marchés financiers / Sales & Trading"),
        # IB
        ("investment banking",  "Financement et Investissement"),
        ("corporate banking",   "Financement et Investissement"),
        ("leveraged finance",   "Financement et Investissement"),
        ("project finance",     "Financement et Investissement"),
        ("infrastructure",      "Financement et Investissement"),
        ("loan",                "Financement et Investissement"),
        ("syndication",         "Financement et Investissement"),
        ("m&a",                 "Financement et Investissement"),
        ("debt capital",        "Financement et Investissement"),
    ]

    for keyword, family in title_rules:
        if keyword in title_l:
            return family

    # Fallback sur combined (titre + début description)
    for keyword, family in FAMILY_MAP.items():
        if keyword in combined:
            return family

    return "Autres"


def classify_level(title: str) -> str:
    """Détermine le niveau d'expérience depuis le titre."""
    for pattern, level in LEVEL_PATTERNS:
        if pattern.search(title):
            return level
    return "Non spécifié"


def classify_contract(title: str, time_type: str = "") -> str:
    """Détermine le type de contrat."""
    combined = f"{title} {time_type}".lower()
    if re.search(r"\bstage\b|\bintern(?:ship)?\b|\boff[\s-]cycle\b|\bsummer\s+analyst\b", combined):
        return "Stage"
    if re.search(r"\balternance\b|\bapprentice\b|\bworking\s+student\b", combined):
        return "Alternance"
    if re.search(r"\bfixed[\s-]term\b|\bftc\b|\btemporary\b|\bcdd\b|\bcontract(?:or|ed)?\b", combined):
        return "CDD"
    if re.search(r"\bvie\b", combined):
        return "VIE"
    if re.search(r"\bpart[\s-]time\b", combined):
        return "CDI"
    if time_type and "part" in time_type.lower():
        return "CDI"
    return "CDI"


def normalize_entity(raw: str) -> str:
    """Normalise les noms d'entités Mizuho."""
    if not raw:
        return "Mizuho"
    r = raw.strip()
    # Mizuho Americas Services LLC / Mizuho Capital Markets LLC / ...
    if re.search(r"America", r, re.I):
        return "Mizuho Americas"
    if re.search(r"Securities|Capital\s+Market", r, re.I):
        return "Mizuho Securities"
    if re.search(r"International|EMEA|Europe", r, re.I):
        return "Mizuho International"
    if re.search(r"Bank", r, re.I):
        return "Mizuho Bank"
    return "Mizuho"


def today_str() -> str:
    return date.today().isoformat()


def now_str() -> str:
    return datetime.now().isoformat(timespec="seconds")


# ── Base SQLite ───────────────────────────────────────────────────────────────
SCHEMA = """
CREATE TABLE IF NOT EXISTS jobs (
    job_id          TEXT PRIMARY KEY,
    source          TEXT,
    candidate_type  TEXT,
    entity          TEXT,
    job_title       TEXT,
    job_family      TEXT,
    city            TEXT,
    country         TEXT,
    region          TEXT,
    contract_type   TEXT,
    experience_level TEXT,
    education_level TEXT,
    description     TEXT,
    offer_url       TEXT,
    posted_date     TEXT,
    scraped_at      TEXT
);
CREATE INDEX IF NOT EXISTS idx_country  ON jobs(country);
CREATE INDEX IF NOT EXISTS idx_family   ON jobs(job_family);
CREATE INDEX IF NOT EXISTS idx_contract ON jobs(contract_type);
CREATE INDEX IF NOT EXISTS idx_source   ON jobs(source);
"""


def init_db(conn: sqlite3.Connection) -> None:
    conn.executescript(SCHEMA)
    conn.commit()


def upsert_jobs(conn: sqlite3.Connection, rows: list[dict]) -> int:
    inserted = 0
    for r in rows:
        try:
            conn.execute(
                """INSERT INTO jobs
                   (job_id, source, candidate_type, entity, job_title, job_family,
                    city, country, region, contract_type, experience_level,
                    education_level, description, offer_url, posted_date, scraped_at)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                   ON CONFLICT(job_id) DO UPDATE SET
                     job_title=excluded.job_title, job_family=excluded.job_family,
                     city=excluded.city, country=excluded.country,
                     region=excluded.region, contract_type=excluded.contract_type,
                     experience_level=excluded.experience_level,
                     education_level=excluded.education_level,
                     description=excluded.description,
                     offer_url=excluded.offer_url,
                     posted_date=excluded.posted_date,
                     scraped_at=excluded.scraped_at""",
                (
                    r["job_id"], r["source"], r["candidate_type"], r["entity"],
                    r["job_title"], r["job_family"], r["city"], r["country"],
                    r["region"], r["contract_type"], r["experience_level"],
                    r["education_level"], r["description"], r["offer_url"],
                    r["posted_date"], r["scraped_at"],
                ),
            )
            if conn.execute("SELECT changes()").fetchone()[0]:
                inserted += 1
        except Exception as exc:
            log.warning("upsert error %s: %s", r.get("job_id"), exc)
    conn.commit()
    return inserted


# ── Workday helpers ───────────────────────────────────────────────────────────
def _wd_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "Content-Type": "application/json",
        "Accept":       "application/json",
        "User-Agent":   "Mozilla/5.0 (compatible; Taleos-Scraper/1.0)",
    })
    return s


def wd_fetch_all_listings(src: dict, session: requests.Session) -> list[dict]:
    """Pagine l'API Workday pour récupérer toutes les offres d'une source.
    Note: Workday ne retourne le total que sur la page 0 — on mémorise
    et on continue tant qu'une page est pleine (== PAGE_SIZE résultats)."""
    host = src["host"]
    tenant = src["tenant"]
    site   = src["site"]
    api_url = f"https://{host}/wday/cxs/{tenant}/{site}/jobs"
    listings, offset, known_total = [], 0, None
    while True:
        payload = {"limit": PAGE_SIZE, "offset": offset, "searchText": "", "appliedFacets": {}}
        try:
            resp = session.post(api_url, json=payload, timeout=30)
            resp.raise_for_status()
            data = resp.json()
        except Exception as exc:
            log.error("[%s] fetch error offset=%d: %s", src["id"], offset, exc)
            break
        postings = data.get("jobPostings", [])
        if not postings:
            break
        for p in postings:
            p["_source"] = src["id"]
        listings.extend(postings)
        # Le total n'est fiable que sur la page 0
        page_total = data.get("total")
        if page_total and page_total > 0:
            known_total = page_total
        log.info("[%s] %d/%s offres récupérées",
                 src["id"], len(listings), known_total or "?")
        # Arrêter si : page incomplète OU on a atteint le total connu
        if len(postings) < PAGE_SIZE:
            break
        if known_total and len(listings) >= known_total:
            break
        offset += PAGE_SIZE
        time.sleep(REQUEST_DELAY)
    return listings


def wd_fetch_detail(src: dict, external_path: str, session: requests.Session) -> dict | None:
    """Récupère le détail d'une offre Workday."""
    host   = src["host"]
    tenant = src["tenant"]
    site   = src["site"]
    # external_path = "/job/NYC-1285/Title_R123"  →  strip "/job/"
    path = re.sub(r"^/job/", "", external_path)
    url  = f"https://{host}/wday/cxs/{tenant}/{site}/job/{path}"
    try:
        resp = session.get(url, timeout=30)
        resp.raise_for_status()
        return resp.json().get("jobPostingInfo")
    except Exception as exc:
        log.warning("[%s] detail error %s: %s", src["id"], external_path, exc)
        return None


def parse_wd_job(listing: dict, detail: dict | None, src: dict) -> dict | None:
    """Construit un enregistrement normalisé depuis les données Workday."""
    title = (detail or listing).get("title") or listing.get("title", "")
    if not title:
        return None

    # ── Localisation ──
    alpha2, country_name, location_desc = None, None, ""
    if detail:
        loc = detail.get("jobRequisitionLocation", {}) or {}
        country_obj = loc.get("country") or {}
        alpha2       = country_obj.get("alpha2Code")
        country_name = country_obj.get("descriptor")
        location_desc = loc.get("descriptor") or detail.get("location") or ""
    if not location_desc:
        location_desc = listing.get("locationsText", "")
    city = extract_city(location_desc)
    country_fr, region = resolve_country(alpha2, country_name, city)
    # Fallback region depuis la source
    if region == "Non spécifié" and src.get("region"):
        region = src["region"]

    # ── Entité ──
    entity = ""
    if detail:
        # detail n'a pas hiringOrganization directement, on le prend depuis listing
        pass
    entity = listing.get("_entity") or ""
    entity = normalize_entity(entity) if entity else "Mizuho"

    # ── Contrat / type de temps ──
    time_type = (detail or {}).get("timeType", "") if detail else ""
    contract_type = classify_contract(title, time_type)

    # ── Famille métier ──
    desc_html = (detail or {}).get("jobDescription", "") if detail else ""
    desc_text = clean_html(desc_html)
    job_family = classify_family(title, desc_text[:500])

    # ── Expérience ──
    exp_level = classify_level(title)

    # ── Candidat ──
    candidate_type = "Campus" if CAMPUS_PATTERNS.search(title) else "Experienced"

    # ── Éducation ──
    edu = ""
    if re.search(r"\bGraduate\b|\bIntern\b|\bUndergrad", title, re.I):
        edu = "Bac +3 / Bac +5"
    elif re.search(r"\bPhD\b|\bDoctora", title, re.I):
        edu = "Bac +8"

    # ── URL ──
    ext_path = listing.get("externalPath", "")
    host = src["host"]
    site = src["site"]
    tenant = src["tenant"]
    offer_url = f"https://{host}/{site}/job{ext_path}" if ext_path else ""

    # ── Date ──
    posted_raw = (detail or listing).get("startDate") or listing.get("postedOn") or ""
    posted_date = posted_raw[:10] if posted_raw and len(posted_raw) >= 10 else today_str()
    # "Posted Today" / "Posted X days ago" → today
    if not re.match(r"\d{4}-\d{2}-\d{2}", posted_date):
        posted_date = today_str()

    # ── job_id ──
    job_id_raw = (detail or {}).get("jobReqId") or (detail or {}).get("id") or ""
    if not job_id_raw:
        job_id_raw = re.search(r"_([A-Z]\d+)$", ext_path or "")
        job_id_raw = job_id_raw.group(1) if job_id_raw else re.sub(r"[^A-Za-z0-9]", "_", ext_path or "")[-30:]
    job_id = f"mizuho_wd_{job_id_raw}"

    return {
        "job_id":          job_id,
        "source":          f"mizuho_{src['id']}",
        "candidate_type":  candidate_type,
        "entity":          entity,
        "job_title":       title,
        "job_family":      job_family,
        "city":            city,
        "country":         country_fr,
        "region":          region,
        "contract_type":   contract_type,
        "experience_level": exp_level,
        "education_level": edu,
        "description":     desc_text[:3000],
        "offer_url":       offer_url,
        "posted_date":     posted_date,
        "scraped_at":      now_str(),
    }


def scrape_workday(src: dict, conn: sqlite3.Connection) -> int:
    """Scrape une source Workday et insère en DB. Retourne le nb d'offres upsertées."""
    session = _wd_session()
    log.info("=== Workday [%s] ===", src["id"])

    listings = wd_fetch_all_listings(src, session)
    log.info("[%s] %d offres listées, début des détails…", src["id"], len(listings))

    # Récupérer l'entité hiringOrganization via un premier détail
    # (le listing seul ne le contient pas)
    jobs_parsed = []
    for i, listing in enumerate(listings):
        ext_path = listing.get("externalPath", "")
        detail   = wd_fetch_detail(src, ext_path, session)
        if detail is None:
            log.warning("[%s] détail manquant pour %s", src["id"], ext_path)
            detail = {}

        # Récupérer hiringOrganization (dans la réponse brute du detail endpoint)
        # On refait la requête pour obtenir la réponse complète
        path = re.sub(r"^/job/", "", ext_path)
        host = src["host"]; tenant = src["tenant"]; site = src["site"]
        detail_url = f"https://{host}/wday/cxs/{tenant}/{site}/job/{path}"
        try:
            r2 = session.get(detail_url, timeout=30)
            full = r2.json()
            entity_raw = full.get("hiringOrganization", {}).get("name", "")
        except Exception:
            entity_raw = ""
        listing["_entity"] = entity_raw

        job = parse_wd_job(listing, detail, src)
        if job:
            jobs_parsed.append(job)

        if (i + 1) % 10 == 0:
            log.info("[%s] %d/%d détails traités", src["id"], i + 1, len(listings))
        time.sleep(REQUEST_DELAY)

    inserted = upsert_jobs(conn, jobs_parsed)
    log.info("[%s] %d offres insérées/mises à jour", src["id"], inserted)
    return len(jobs_parsed)


# ── EMEA SF scraper (HTML) ────────────────────────────────────────────────────

def emea_fetch_listings(session: requests.Session) -> list[dict]:
    """Récupère la liste des offres EMEA depuis la page de résultats SF."""
    try:
        resp = session.get(EMEA_SEARCH_URL, timeout=30)
        resp.raise_for_status()
        html = resp.text
    except Exception as exc:
        log.error("[EMEA] fetch listings error: %s", exc)
        return []

    # Liens /job/City-Title/JobID/
    pattern = re.compile(r'href=["\'](?P<path>/job/(?P<slug>[^"\'<>\s/]+)/(?P<jid>\d+)/)["\']')
    seen, jobs = set(), []
    for m in pattern.finditer(html):
        jid = m.group("jid")
        if jid in seen:
            continue
        seen.add(jid)
        slug = unquote(html_lib.unescape(m.group("slug")))
        jobs.append({
            "job_id_raw": jid,
            "slug":       slug,
            "path":       m.group("path"),
        })
    log.info("[EMEA] %d offres listées", len(jobs))
    return jobs


def emea_fetch_detail(path: str, session: requests.Session) -> dict:
    """Scrape la page de détail d'une offre EMEA."""
    url = f"{EMEA_BASE_URL}{path}"
    try:
        resp = session.get(url, timeout=30)
        resp.raise_for_status()
        html = resp.text
    except Exception as exc:
        log.warning("[EMEA] detail error %s: %s", path, exc)
        return {"url": url}

    # Titre
    m_title = re.search(r"<title[^>]*>(.*?)(?:\s*\|[^<]*|\s*Job Details[^<]*)?\s*</title>", html, re.I | re.S)
    title = clean_html(m_title.group(1)).strip() if m_title else ""
    title = re.sub(r"\s*Job Details\s*$", "", title, flags=re.I).strip()

    # Localisation — span.jobGeoLocation = "London (Lon), GB"
    m_geo = re.search(r'<span[^>]*class="jobGeoLocation"[^>]*>(.*?)</span>', html, re.I | re.S)
    geo_text = clean_html(m_geo.group(1)).strip() if m_geo else ""
    # "London (Lon), GB" → city=London, alpha2=GB
    m_geo2 = re.match(r"^([^(,]+?)(?:\s*\([^)]*\))?,\s*([A-Z]{2})$", geo_text)
    if m_geo2:
        city_raw  = m_geo2.group(1).strip()
        alpha2    = m_geo2.group(2).strip()
    else:
        # Fallback : première partie du slug = ville
        slug_parts = path.strip("/").split("/")
        city_raw   = slug_parts[1].split("-")[0] if len(slug_parts) > 1 else ""
        alpha2     = None

    # Date
    m_date = re.search(r"Date:\s*</[^>]+>\s*<[^>]+>\s*([^<]{5,30})", html, re.I)
    if not m_date:
        m_date = re.search(r"(\d{1,2}\s+(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{4})", html, re.I)
    posted_raw = m_date.group(1).strip() if m_date else today_str()
    # Parser la date en ISO
    try:
        from datetime import datetime as dt
        for fmt in ("%d %B %Y", "%d %b %Y"):
            try:
                posted_date = dt.strptime(posted_raw, fmt).strftime("%Y-%m-%d")
                break
            except ValueError:
                continue
        else:
            posted_date = today_str()
    except Exception:
        posted_date = today_str()

    # Description — le plus grand bloc de texte
    all_divs = re.findall(r"<div[^>]*>(.*?)</div>", html, re.I | re.S)
    desc_text = ""
    for div in all_divs:
        t = clean_html(div).strip()
        if len(t) > len(desc_text):
            desc_text = t
    # Tronquer et filtrer le nav / cookie text
    if "Required Cookies" in desc_text or "Home Page" in desc_text:
        # Prendre le 2e plus grand bloc
        sorted_divs = sorted(all_divs, key=lambda d: len(clean_html(d)), reverse=True)
        for div in sorted_divs[1:]:
            t = clean_html(div).strip()
            if "Required Cookies" not in t and len(t) > 200:
                desc_text = t
                break

    return {
        "title":       title,
        "city":        city_raw,
        "alpha2":      alpha2,
        "posted_date": posted_date,
        "description": desc_text[:3000],
        "url":         url,
    }


def parse_emea_job(listing: dict, detail: dict) -> dict | None:
    """Construit un enregistrement normalisé depuis les données EMEA."""
    title = detail.get("title") or ""
    if not title:
        # Fallback : reconstruire depuis le slug
        slug = listing.get("slug", "")
        # Enlever la ville du début du slug
        title = re.sub(r"^[A-Za-z]+[-]", "", slug).replace("-", " ").strip()
    if not title:
        return None

    city   = detail.get("city") or extract_city(listing.get("slug", "").split("-")[0])
    alpha2 = detail.get("alpha2")
    country_fr, region = resolve_country(alpha2, None, city)

    contract_type = classify_contract(title)
    job_family    = classify_family(title, detail.get("description", "")[:500])
    exp_level     = classify_level(title)
    candidate_type = "Campus" if CAMPUS_PATTERNS.search(title) else "Experienced"
    edu = ""
    if re.search(r"\bGraduate\b|\bIntern\b", title, re.I):
        edu = "Bac +3 / Bac +5"

    job_id = f"mizuho_emea_{listing['job_id_raw']}"

    return {
        "job_id":          job_id,
        "source":          "mizuho_emea",
        "candidate_type":  candidate_type,
        "entity":          "Mizuho International",
        "job_title":       title,
        "job_family":      job_family,
        "city":            city,
        "country":         country_fr,
        "region":          region,
        "contract_type":   contract_type,
        "experience_level": exp_level,
        "education_level": edu,
        "description":     detail.get("description", "")[:3000],
        "offer_url":       detail.get("url", ""),
        "posted_date":     detail.get("posted_date") or today_str(),
        "scraped_at":      now_str(),
    }


def scrape_emea(conn: sqlite3.Connection) -> int:
    """Scrape EMEA et insère en DB. Retourne le nb d'offres traitées."""
    session = requests.Session()
    session.headers.update({
        "User-Agent":  "Mozilla/5.0 (compatible; Taleos-Scraper/1.0)",
        "Accept":      "text/html,application/xhtml+xml",
        "Accept-Language": "en-GB,en;q=0.9",
    })
    log.info("=== EMEA (careers.mizuhoemea.com) ===")

    listings = emea_fetch_listings(session)
    jobs_parsed = []
    for i, listing in enumerate(listings):
        detail = emea_fetch_detail(listing["path"], session)
        job = parse_emea_job(listing, detail)
        if job:
            jobs_parsed.append(job)
        log.debug("[EMEA] %d/%d: %s", i + 1, len(listings), detail.get("title", "?")[:50])
        time.sleep(REQUEST_DELAY)

    inserted = upsert_jobs(conn, jobs_parsed)
    log.info("[EMEA] %d offres insérées/mises à jour", inserted)
    return len(jobs_parsed)


# ── Statistiques ──────────────────────────────────────────────────────────────
def print_stats(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()
    total = cur.execute("SELECT COUNT(*) FROM jobs").fetchone()[0]
    print(f"\n{'='*60}")
    print(f"  Mizuho — {total} offres en base")
    print(f"{'='*60}")

    sections = [
        ("── Par source ──",       "SELECT source, COUNT(*) n FROM jobs GROUP BY source ORDER BY n DESC"),
        ("── Par région ──",       "SELECT region, COUNT(*) n FROM jobs GROUP BY region ORDER BY n DESC"),
        ("── Par pays ──",         "SELECT country, COUNT(*) n FROM jobs GROUP BY country ORDER BY n DESC"),
        ("── Par contrat ──",      "SELECT contract_type, COUNT(*) n FROM jobs GROUP BY contract_type ORDER BY n DESC"),
        ("── Par expérience ──",   "SELECT experience_level, COUNT(*) n FROM jobs GROUP BY experience_level ORDER BY n DESC"),
        ("── Par famille ──",      "SELECT job_family, COUNT(*) n FROM jobs GROUP BY job_family ORDER BY n DESC"),
        ("── Par candidat ──",     "SELECT candidate_type, COUNT(*) n FROM jobs GROUP BY candidate_type ORDER BY n DESC"),
    ]
    for header, query in sections:
        print(f"\n{header}")
        for row in cur.execute(query).fetchall():
            print(f"  {row[1]:5}  {row[0]}")

    # Offres avec champs non spécifiés
    print("\n── Champs non spécifiés ──")
    for col in ("country", "region", "contract_type", "experience_level", "job_family"):
        n = cur.execute(
            f"SELECT COUNT(*) FROM jobs WHERE {col} IS NULL OR {col} IN ('','Non spécifié','Autres')"
        ).fetchone()[0]
        print(f"  {col:25} : {n} non spécifiés")


# ── Export CSV ───────────────────────────────────────────────────────────────
def export_csv(conn: sqlite3.Connection) -> None:
    out = SCRIPT_DIR / "mizuho_jobs_export.csv"
    cols = ["job_id", "source", "candidate_type", "entity", "job_title",
            "job_family", "city", "country", "region", "contract_type",
            "experience_level", "education_level", "offer_url", "posted_date"]
    rows = conn.execute(f"SELECT {','.join(cols)} FROM jobs ORDER BY posted_date DESC").fetchall()
    with open(out, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(cols)
        w.writerows(rows)
    log.info("CSV exporté : %s (%d lignes)", out, len(rows))


# ── Main ─────────────────────────────────────────────────────────────────────
def main() -> None:
    parser = argparse.ArgumentParser(description="Mizuho scraper")
    parser.add_argument("--stats-only",  action="store_true", help="Affiche les stats sans scraper")
    parser.add_argument("--export-csv",  action="store_true", help="Exporte un CSV après scrape")
    parser.add_argument("--source",      choices=["americas", "apac", "emea", "all"], default="all",
                        help="Source à scraper (défaut: all)")
    args = parser.parse_args()

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    init_db(conn)

    if args.stats_only:
        print_stats(conn)
        conn.close()
        return

    total = 0
    sources_to_run = args.source if args.source != "all" else "all"

    for src in SOURCES_WD:
        if sources_to_run == "all" or src["id"] == sources_to_run:
            total += scrape_workday(src, conn)

    if sources_to_run in ("all", "emea"):
        total += scrape_emea(conn)

    log.info("=== Total : %d offres traitées ===", total)
    print_stats(conn)

    if args.export_csv:
        export_csv(conn)

    conn.close()


if __name__ == "__main__":
    main()
