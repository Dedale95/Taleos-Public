#!/usr/bin/env python3
"""
Bank of America — Scraper multi-sources
=======================================
Sources :
  1. Workday Lateral-EMEA  : ghr.wd1.myworkdayjobs.com/Lateral-EMEA  (~35 offres)
  2. Workday Lateral-APAC  : ghr.wd1.myworkdayjobs.com/lateral-apac   (~72 offres)
  3. Workday Lateral-US    : ghr.wd1.myworkdayjobs.com/Lateral-US     (~1527 offres)
  4. Campus Atom Feed      : bankcampuscareers.tal.net/...             (~10-50 programmes)

Stratégie :
  - EMEA + APAC : appel detail API pour chaque offre (pays exact)
  - US           : extraction ville depuis externalPath, pays=US (pas de detail)
  - Campus       : parse Atom/RSS feed (tous les champs disponibles directement)
"""

from __future__ import annotations

import hashlib
import logging
import re
import sqlite3
import time
import xml.etree.ElementTree as ET
from datetime import datetime
from pathlib import Path
from typing import Optional

import requests

# ─────────────────────────────── Logging ────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(Path(__file__).parent / "bank_of_america_scraper.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger(__name__)

# ─────────────────────────────── Config ─────────────────────────────────────
DB_PATH       = Path(__file__).parent / "bank_of_america_jobs.db"
PAGE_SIZE     = 20
REQUEST_DELAY = 0.5
TIMEOUT       = 20

WORKDAY_HOST  = "ghr.wd1.myworkdayjobs.com"
WORKDAY_TENANT = "ghr"

# Sources Workday (tenant ghr)
WORKDAY_SOURCES = [
    {
        "id":           "lateral-emea",
        "site":         "Lateral-EMEA",
        "label":        "Expérimentés EMEA",
        "region_default": "Europe",
        "fetch_detail": True,
    },
    {
        "id":           "lateral-apac",
        "site":         "lateral-apac",
        "label":        "Expérimentés APAC",
        "region_default": "Asie-Pacifique",
        "fetch_detail": True,
    },
    {
        "id":           "lateral-us",
        "site":         "Lateral-US",
        "label":        "Expérimentés Amérique du Nord",
        "region_default": "Amérique du Nord",
        "fetch_detail": False,   # trop d'offres (~1500) → ville depuis externalPath
    },
]

CAMPUS_FEED_URL = "https://bankcampuscareers.tal.net/vx/mobile-0/candidate/jobboard/vacancy/1/feed"
CAMPUS_BASE_URL = "https://bankcampuscareers.tal.net"

ATOM_NS = "http://www.w3.org/2005/Atom"

# ─────────────────────────── Correspondances pays ───────────────────────────
# Workday country.descriptor (anglais) → (pays_fr, région)
COUNTRY_EN_MAP: dict[str, tuple[str, str]] = {
    "united states of america": ("États-Unis",       "Amérique du Nord"),
    "united states":            ("États-Unis",       "Amérique du Nord"),
    "canada":                   ("Canada",           "Amérique du Nord"),
    "mexico":                   ("Mexique",          "Amérique du Nord"),
    "united kingdom":           ("Royaume-Uni",      "Europe"),
    "germany":                  ("Allemagne",        "Europe"),
    "france":                   ("France",           "Europe"),
    "netherlands":              ("Pays-Bas",         "Europe"),
    "spain":                    ("Espagne",          "Europe"),
    "italy":                    ("Italie",           "Europe"),
    "switzerland":              ("Suisse",           "Europe"),
    "luxembourg":               ("Luxembourg",       "Europe"),
    "ireland":                  ("Irlande",          "Europe"),
    "poland":                   ("Pologne",          "Europe"),
    "belgium":                  ("Belgique",         "Europe"),
    "sweden":                   ("Suède",            "Europe"),
    "denmark":                  ("Danemark",         "Europe"),
    "finland":                  ("Finlande",         "Europe"),
    "norway":                   ("Norvège",          "Europe"),
    "portugal":                 ("Portugal",         "Europe"),
    "czech republic":           ("République tchèque", "Europe"),
    "hungary":                  ("Hongrie",          "Europe"),
    "romania":                  ("Roumanie",         "Europe"),
    "austria":                  ("Autriche",         "Europe"),
    "united arab emirates":     ("Émirats arabes unis", "Moyen-Orient / Afrique"),
    "saudi arabia":             ("Arabie saoudite",  "Moyen-Orient / Afrique"),
    "qatar":                    ("Qatar",            "Moyen-Orient / Afrique"),
    "bahrain":                  ("Bahreïn",          "Moyen-Orient / Afrique"),
    "south africa":             ("Afrique du Sud",   "Moyen-Orient / Afrique"),
    "japan":                    ("Japon",            "Asie-Pacifique"),
    "hong kong":                ("Hong Kong",        "Asie-Pacifique"),
    "hong kong sar":            ("Hong Kong",        "Asie-Pacifique"),
    "singapore":                ("Singapour",        "Asie-Pacifique"),
    "china":                    ("Chine",            "Asie-Pacifique"),
    "india":                    ("Inde",             "Asie-Pacifique"),
    "australia":                ("Australie",        "Asie-Pacifique"),
    "korea, republic of":       ("Corée du Sud",     "Asie-Pacifique"),
    "south korea":              ("Corée du Sud",     "Asie-Pacifique"),
    "taiwan":                   ("Taïwan",           "Asie-Pacifique"),
    "thailand":                 ("Thaïlande",        "Asie-Pacifique"),
    "malaysia":                 ("Malaisie",         "Asie-Pacifique"),
    "indonesia":                ("Indonésie",        "Asie-Pacifique"),
    "philippines":              ("Philippines",      "Asie-Pacifique"),
    "vietnam":                  ("Vietnam",          "Asie-Pacifique"),
    "new zealand":              ("Nouvelle-Zélande", "Asie-Pacifique"),
}

# Villes → (pays_fr, région) — fallback quand l'API ne donne pas le pays
CITY_COUNTRY_MAP: dict[str, tuple[str, str]] = {
    # USA
    "new york":        ("États-Unis", "Amérique du Nord"),
    "charlotte":       ("États-Unis", "Amérique du Nord"),
    "chicago":         ("États-Unis", "Amérique du Nord"),
    "san francisco":   ("États-Unis", "Amérique du Nord"),
    "boston":          ("États-Unis", "Amérique du Nord"),
    "dallas":          ("États-Unis", "Amérique du Nord"),
    "plano":           ("États-Unis", "Amérique du Nord"),
    "washington":      ("États-Unis", "Amérique du Nord"),
    "atlanta":         ("États-Unis", "Amérique du Nord"),
    "phoenix":         ("États-Unis", "Amérique du Nord"),
    "jacksonville":    ("États-Unis", "Amérique du Nord"),
    "los angeles":     ("États-Unis", "Amérique du Nord"),
    "seattle":         ("États-Unis", "Amérique du Nord"),
    "houston":         ("États-Unis", "Amérique du Nord"),
    "miami":           ("États-Unis", "Amérique du Nord"),
    "denver":          ("États-Unis", "Amérique du Nord"),
    "minneapolis":     ("États-Unis", "Amérique du Nord"),
    "richmond":        ("États-Unis", "Amérique du Nord"),
    "jersey city":     ("États-Unis", "Amérique du Nord"),
    # Canada
    "toronto":         ("Canada",     "Amérique du Nord"),
    "montreal":        ("Canada",     "Amérique du Nord"),
    "vancouver":       ("Canada",     "Amérique du Nord"),
    # UK
    "london":          ("Royaume-Uni", "Europe"),
    "bromley":         ("Royaume-Uni", "Europe"),
    "chester":         ("Royaume-Uni", "Europe"),
    "birmingham":      ("Royaume-Uni", "Europe"),
    "edinburgh":       ("Royaume-Uni", "Europe"),
    "manchester":      ("Royaume-Uni", "Europe"),
    # Europe
    "paris":           ("France",     "Europe"),
    "frankfurt":       ("Allemagne",  "Europe"),
    "amsterdam":       ("Pays-Bas",   "Europe"),
    "madrid":          ("Espagne",    "Europe"),
    "milan":           ("Italie",     "Europe"),
    "zurich":          ("Suisse",     "Europe"),
    "dublin":          ("Irlande",    "Europe"),
    "luxembourg":      ("Luxembourg", "Europe"),
    "warsaw":          ("Pologne",    "Europe"),
    "brussels":        ("Belgique",   "Europe"),
    # APAC
    "tokyo":           ("Japon",      "Asie-Pacifique"),
    "hong kong":       ("Hong Kong",  "Asie-Pacifique"),
    "singapore":       ("Singapour",  "Asie-Pacifique"),
    "sydney":          ("Australie",  "Asie-Pacifique"),
    "mumbai":          ("Inde",       "Asie-Pacifique"),
    "bangalore":       ("Inde",       "Asie-Pacifique"),
    "hyderabad":       ("Inde",       "Asie-Pacifique"),
    "shanghai":        ("Chine",      "Asie-Pacifique"),
    "beijing":         ("Chine",      "Asie-Pacifique"),
    "seoul":           ("Corée du Sud", "Asie-Pacifique"),
    "taipei":          ("Taïwan",     "Asie-Pacifique"),
    "kuala lumpur":    ("Malaisie",   "Asie-Pacifique"),
    "jakarta":         ("Indonésie",  "Asie-Pacifique"),
    "manila":          ("Philippines", "Asie-Pacifique"),
    # MENA
    "dubai":           ("Émirats arabes unis", "Moyen-Orient / Afrique"),
    "abu dhabi":       ("Émirats arabes unis", "Moyen-Orient / Afrique"),
    "riyadh":          ("Arabie saoudite", "Moyen-Orient / Afrique"),
    "doha":            ("Qatar",      "Moyen-Orient / Afrique"),
    "johannesburg":    ("Afrique du Sud", "Moyen-Orient / Afrique"),
}

# ─────────────────────── Classification métier ───────────────────────────────
FAMILY_RULES: list[tuple[str, str]] = [
    # Ordre : spécifique → général
    ("audit",                      "Inspection / Audit"),
    ("financial crime",            "Conformité / Sécurité financière"),
    ("kyc",                        "Conformité / Sécurité financière"),
    ("aml",                        "Conformité / Sécurité financière"),
    ("anti-money",                 "Conformité / Sécurité financière"),
    ("compliance",                 "Conformité / Sécurité financière"),
    ("attorney",                   "Juridique"),
    ("counsel",                    "Juridique"),
    ("legal",                      "Juridique"),
    # IT — noms canoniques (sous-classifié au moment de l'export)
    ("software engineer",          "IT, Digital et Data"),
    ("developer",                  "IT, Digital et Data"),
    ("data engineer",              "IT, Digital et Data"),
    ("data scientist",             "IT, Digital et Data"),
    ("machine learning",           "IT, Digital et Data"),
    ("artificial intelligence",    "IT, Digital et Data"),
    ("cybersecurity",              "IT, Digital et Data"),
    ("infrastructure",             "IT, Digital et Data"),
    ("cloud",                      "IT, Digital et Data"),
    ("technology",                 "IT, Digital et Data"),
    ("digital",                    "IT, Digital et Data"),
    ("software",                   "IT, Digital et Data"),
    ("platform engineer",          "IT, Digital et Data"),
    # Risk
    ("credit risk",                "Risques / Contrôles permanents"),
    ("market risk",                "Risques / Contrôles permanents"),
    ("operational risk",           "Risques / Contrôles permanents"),
    ("quantitative risk",          "Risques / Contrôles permanents"),
    ("risk management",            "Risques / Contrôles permanents"),
    ("risk analyst",               "Risques / Contrôles permanents"),
    ("risk",                       "Risques / Contrôles permanents"),
    # Finance / Compta
    ("accounting",                 "Finances / Comptabilité / Contrôle de gestion"),
    ("financial reporting",        "Finances / Comptabilité / Contrôle de gestion"),
    ("regulatory reporting",       "Finances / Comptabilité / Contrôle de gestion"),
    ("tax",                        "Finances / Comptabilité / Contrôle de gestion"),
    ("treasury",                   "Finances / Comptabilité / Contrôle de gestion"),
    ("controller",                 "Finances / Comptabilité / Contrôle de gestion"),
    ("finance",                    "Finances / Comptabilité / Contrôle de gestion"),
    # Opérations
    ("operations",                 "Gestion des opérations"),
    ("settlements",                "Gestion des opérations"),
    ("collateral",                 "Gestion des opérations"),
    ("loan agency",                "Gestion des opérations"),
    ("trade support",              "Gestion des opérations"),
    ("transaction banking",        "Gestion des opérations"),
    ("cash management",            "Gestion des opérations"),
    ("custody",                    "Gestion des opérations"),
    ("transfer agent",             "Gestion des opérations"),
    ("client service",             "Gestion des opérations"),
    # RH
    ("human resources",            "Ressources Humaines"),
    ("talent acquisition",         "Ressources Humaines"),
    ("recruiting",                 "Ressources Humaines"),
    ("people",                     "Ressources Humaines"),
    # Marketing & Communication
    ("communications",             "Marketing et Communication"),
    ("marketing",                  "Marketing et Communication"),
    ("brand",                      "Marketing et Communication"),
    # Stratégie / Direction
    ("strategy",                   "Direction générale"),
    ("chief of staff",             "Direction générale"),
    ("program manager",            "Direction générale"),
    ("project manager",            "Organisation / Qualité"),
    ("business analyst",           "Organisation / Qualité"),
    # Marchés financiers & Trading
    ("sales & trading",            "Marchés financiers & Trading"),
    ("structured products",        "Marchés financiers & Trading"),
    ("derivatives",                "Marchés financiers & Trading"),
    ("fixed income",               "Marchés financiers & Trading"),
    ("equity research",            "Marchés financiers & Trading"),
    ("equities",                   "Marchés financiers & Trading"),
    ("securities",                 "Marchés financiers & Trading"),
    ("trading",                    "Marchés financiers & Trading"),
    ("research analyst",           "Marchés financiers & Trading"),
    ("global markets",             "Marchés financiers & Trading"),
    ("sales",                      "Marchés financiers & Trading"),
    ("markets",                    "Marchés financiers & Trading"),
    # IB / Corporate Banking
    ("investment banking",         "Financement et Investissement"),
    ("global corporate",           "Financement et Investissement"),
    ("global investment banking",  "Financement et Investissement"),
    ("capital markets",            "Financement et Investissement"),
    ("leveraged finance",          "Financement et Investissement"),
    ("project finance",            "Financement et Investissement"),
    ("acquisition finance",        "Financement et Investissement"),
    ("m&a",                        "Financement et Investissement"),
    ("mergers",                    "Financement et Investissement"),
    ("corporate banking",          "Financement et Investissement"),
    ("loan syndication",           "Financement et Investissement"),
    ("coverage",                   "Financement et Investissement"),
    # Banque Privée & Patrimoine / Relation Client
    ("wealth management",          "Banque Privée & Gestion de Patrimoine"),
    ("merrill",                    "Banque Privée & Gestion de Patrimoine"),
    ("private bank",               "Banque Privée & Gestion de Patrimoine"),
    ("financial advisor",          "Banque Privée & Gestion de Patrimoine"),
    ("financial solutions advisor","Banque Privée & Gestion de Patrimoine"),
    ("banker",                     "Banque Privée & Gestion de Patrimoine"),
    ("relationship manager",       "Conseil Clientèle Entreprises"),
    ("relationship banker",        "Conseil Clientèle Entreprises"),
    ("commercial banking",         "Conseil Clientèle Entreprises"),
    ("business banking",           "Conseil Clientèle Entreprises"),
    ("retail",                     "Conseil Clientèle Particuliers"),
    ("branch",                     "Conseil Clientèle Particuliers"),
    ("teller",                     "Conseil Clientèle Particuliers"),
]


def classify_family(title: str) -> str:
    t = title.lower()
    for keyword, family in FAMILY_RULES:
        if keyword in t:
            return family
    return "Autres"


LEVEL_PATTERNS: list[tuple[re.Pattern, str]] = [
    (re.compile(r"\b(managing director|md)\b", re.I), "Senior (10+ ans)"),
    (re.compile(r"\b(executive director|ed)\b", re.I), "Senior (10+ ans)"),
    (re.compile(r"\b(director)\b", re.I),              "Senior (7-10 ans)"),
    (re.compile(r"\b(vice president|vp)\b", re.I),     "Confirmé (5-7 ans)"),
    (re.compile(r"\b(associate|avp)\b", re.I),         "Intermédiaire (3-5 ans)"),
    (re.compile(r"\b(analyst)\b", re.I),               "Junior (0-3 ans)"),
    (re.compile(r"\b(summer analyst|intern)\b", re.I), "Junior (0-3 ans)"),
]


def classify_level(title: str) -> str:
    for pattern, level in LEVEL_PATTERNS:
        if pattern.search(title):
            return level
    return "Non spécifié"


def classify_contract(time_type: str, title: str, source_type: str) -> str:
    """Détermine le type de contrat."""
    title_l = title.lower()
    if source_type == "campus":
        # Campus programs
        if any(k in title_l for k in ["summer", "intern", "winter", "spring"]):
            return "Stage"
        if any(k in title_l for k in ["full time", "full-time", "graduate", "analyst 20"]):
            return "CDI"
        return "Graduate Programme"
    # Lateral
    if time_type and "part" in time_type.lower():
        return "CDD"
    return "CDI"


def classify_education_campus(entry_level: str, description: str) -> str:
    """Éducation pour les programmes campus depuis les champs Taleo."""
    if not entry_level and not description:
        return "Bac +5"
    combined = f"{entry_level} {description}".lower()
    if any(k in combined for k in ["phd", "doctorate", "doctoral"]):
        return "Doctorat"
    if any(k in combined for k in ["mba", "master", "postgraduate", "post-graduate", "m2"]):
        return "Bac +5"
    if any(k in combined for k in ["undergraduate", "bachelor", "licence", "final year"]):
        return "Bac +3"
    return "Bac +5"  # défaut campus


# ─────────────────────────── Helpers réseau ─────────────────────────────────
SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
})


def fetch_json(url: str, payload: dict | None = None, retries: int = 3) -> dict | None:
    for attempt in range(retries):
        try:
            if payload is not None:
                r = SESSION.post(url, json=payload, timeout=TIMEOUT)
            else:
                r = SESSION.get(url, timeout=TIMEOUT)
            if r.status_code == 200:
                return r.json()
            logger.warning(f"HTTP {r.status_code} → {url}")
            return None
        except Exception as e:
            if attempt < retries - 1:
                time.sleep(1.5)
            else:
                logger.error(f"Erreur {url}: {e}")
                return None
    return None


def today_str() -> str:
    return datetime.utcnow().strftime("%Y-%m-%d")


# ─────────────────────────── Extraction ville ───────────────────────────────
def city_from_path(external_path: str) -> str:
    """Extrait la ville depuis externalPath Workday : /job/{City}/{title}_{id}"""
    m = re.match(r"/job/([^/]+)/", external_path or "")
    if not m:
        return ""
    raw = m.group(1).replace("-", " ").strip()
    # Cas "Multiple-Locations" ou "United-States" → pas une vraie ville
    skip = {"multiple locations", "various", "remote", "united states",
            "united kingdom", "virtual", "home", "nationwide"}
    if raw.lower() in skip or len(raw.split()) > 4:
        return ""
    return raw.title()


def resolve_country(country_descriptor: str | None, city: str) -> tuple[str, str]:
    """(pays_fr, région) depuis descriptor Workday ou ville fallback."""
    if country_descriptor:
        key = country_descriptor.lower().strip()
        if key in COUNTRY_EN_MAP:
            return COUNTRY_EN_MAP[key]
    if city:
        key = city.lower().strip()
        if key in CITY_COUNTRY_MAP:
            return CITY_COUNTRY_MAP[key]
        # Essayer sans les articles
        for city_key, val in CITY_COUNTRY_MAP.items():
            if city_key in key or key in city_key:
                return val
    return "Non spécifié", "Non spécifié"


# ──────────────────────────── Scrape Workday ────────────────────────────────
def scrape_workday_source(src: dict, conn: sqlite3.Connection) -> int:
    site = src["site"]
    fetch_detail = src["fetch_detail"]
    region_default = src["region_default"]
    base_api = f"https://{WORKDAY_HOST}/wday/cxs/{WORKDAY_TENANT}/{site}"
    job_url_base = f"https://{WORKDAY_HOST}/fr-FR/{site}"

    logger.info(f"  ▶ {src['label']} ({site}) — detail={'oui' if fetch_detail else 'non'}")

    offset = 0
    total_scraped = 0

    while True:
        payload = {"limit": PAGE_SIZE, "offset": offset}
        data = fetch_json(f"{base_api}/jobs", payload=payload)
        if not data:
            break

        listings = data.get("jobPostings", [])
        if not listings:
            break

        total_api = data.get("total", 0)

        for listing in listings:
            ext_path = listing.get("externalPath", "")
            title = listing.get("title", "").strip()
            if not title or not ext_path:
                continue

            # ID unique
            bullet = listing.get("bulletFields", [])
            req_id = bullet[0] if bullet else ""
            job_id = req_id or re.search(r"_([A-Z0-9]+(?:-\d+)?)$", ext_path or "")
            if hasattr(job_id, "group"):
                job_id = job_id.group(1)
            job_id = str(job_id or "").strip() or hashlib.md5(ext_path.encode()).hexdigest()[:12]

            # URL offre (locale fr-FR)
            offer_url = f"{job_url_base}{ext_path}"

            # Date
            posted_raw = listing.get("postedOn", "")
            posted_date = today_str()
            if posted_raw:
                m_date = re.search(r"(\d{4}-\d{2}-\d{2})", posted_raw)
                if m_date:
                    posted_date = m_date.group(1)
                # "Posted Yesterday" → today - 1, "Posted X Days Ago" → approximatif
                # On garde today_str() comme fallback

            time_type = listing.get("timeType", "Full time")

            # Ville + pays
            city = ""
            country_fr = ""
            region = region_default
            description = ""

            if fetch_detail:
                # Appel detail API
                path_no_job = re.sub(r"^/job/", "", ext_path)
                detail_data = fetch_json(f"{base_api}/job/{path_no_job}")
                time.sleep(REQUEST_DELAY)
                if detail_data:
                    jd = detail_data.get("jobPostingInfo", {})
                    city = jd.get("location", "")
                    country_obj = jd.get("country") or {}
                    country_desc = (
                        country_obj.get("descriptor") if isinstance(country_obj, dict)
                        else str(country_obj)
                    )
                    country_fr, region = resolve_country(country_desc, city)
                    desc_html = jd.get("jobDescription", "") or ""
                    description = re.sub(r"<[^>]+>", " ", desc_html).strip()
                    description = re.sub(r"\s+", " ", description)[:3000]
            else:
                # US : ville depuis externalPath, pays = US
                city = city_from_path(ext_path)
                country_fr = "États-Unis"
                region = "Amérique du Nord"
                time.sleep(REQUEST_DELAY * 0.2)  # pas d'appel detail → délai minimal

            # Vérifier fallback pays depuis ville
            if not country_fr or country_fr == "Non spécifié":
                country_fr, region = resolve_country(None, city)

            job_family = classify_family(title)
            exp_level = classify_level(title)
            contract_type = classify_contract(time_type, title, "lateral")

            _upsert_job(conn, {
                "job_id":         f"bofa_{job_id}",
                "job_title":      title,
                "city":           city,
                "country":        country_fr,
                "region":         region,
                "contract_type":  contract_type,
                "experience_level": exp_level,
                "education_level": "",
                "job_family":     job_family,
                "description":    description,
                "offer_url":      offer_url,
                "posted_date":    posted_date,
                "source":         src["id"],
                "candidate_type": "Experienced",
                "division":       "",
                "entity":         "Bank of America",
            })
            total_scraped += 1

        offset += len(listings)
        if offset >= total_api or len(listings) < PAGE_SIZE:
            break

        if not fetch_detail:
            time.sleep(REQUEST_DELAY)

    logger.info(f"     → {total_scraped} offres scrappées")
    return total_scraped


# ─────────────────────────── Scrape Campus Feed ─────────────────────────────
def parse_campus_field(content_text: str, field: str) -> str:
    """Extrait 'Champ:valeur' depuis le texte de l'entrée Atom."""
    pattern = re.compile(rf"(?:^|\n){re.escape(field)}\s*:\s*(.+?)(?:\n|$)", re.IGNORECASE)
    m = pattern.search(content_text)
    return m.group(1).strip() if m else ""


# Correspondances pays Taleo campus → (pays_fr, région)
CAMPUS_COUNTRY_MAP: dict[str, tuple[str, str]] = {
    "korea, republic of":         ("Corée du Sud",    "Asie-Pacifique"),
    "south korea":                ("Corée du Sud",    "Asie-Pacifique"),
    "australia":                  ("Australie",       "Asie-Pacifique"),
    "hong kong":                  ("Hong Kong",       "Asie-Pacifique"),
    "hong kong sar":              ("Hong Kong",       "Asie-Pacifique"),
    "japan":                      ("Japon",           "Asie-Pacifique"),
    "singapore":                  ("Singapour",       "Asie-Pacifique"),
    "china":                      ("Chine",           "Asie-Pacifique"),
    "india":                      ("Inde",            "Asie-Pacifique"),
    "taiwan":                     ("Taïwan",          "Asie-Pacifique"),
    "thailand":                   ("Thaïlande",       "Asie-Pacifique"),
    "malaysia":                   ("Malaisie",        "Asie-Pacifique"),
    "indonesia":                  ("Indonésie",       "Asie-Pacifique"),
    "philippines":                ("Philippines",     "Asie-Pacifique"),
    "united kingdom":             ("Royaume-Uni",     "Europe"),
    "ireland":                    ("Irlande",         "Europe"),
    "france":                     ("France",          "Europe"),
    "germany":                    ("Allemagne",       "Europe"),
    "spain":                      ("Espagne",         "Europe"),
    "netherlands":                ("Pays-Bas",        "Europe"),
    "luxembourg":                 ("Luxembourg",      "Europe"),
    "switzerland":                ("Suisse",          "Europe"),
    "united arab emirates":       ("Émirats arabes unis", "Moyen-Orient / Afrique"),
    "south africa":               ("Afrique du Sud",  "Moyen-Orient / Afrique"),
    "united states":              ("États-Unis",      "Amérique du Nord"),
    "united states of america":   ("États-Unis",      "Amérique du Nord"),
    "canada":                     ("Canada",          "Amérique du Nord"),
    "brazil":                     ("Brésil",          "Amérique du Nord"),
    "mexico":                     ("Mexique",         "Amérique du Nord"),
}

CAMPUS_REGION_MAP: dict[str, str] = {
    "asia pacific":     "Asie-Pacifique",
    "apac":             "Asie-Pacifique",
    "emea":             "Europe",
    "europe":           "Europe",
    "americas":         "Amérique du Nord",
    "north america":    "Amérique du Nord",
    "latin america":    "Amérique du Nord",
}


def scrape_campus_feed(conn: sqlite3.Connection) -> int:
    logger.info("  ▶ Campus Atom Feed")
    try:
        r = SESSION.get(CAMPUS_FEED_URL, timeout=TIMEOUT)
        r.raise_for_status()
        root = ET.fromstring(r.content)
    except Exception as e:
        logger.error(f"Erreur campus feed: {e}")
        return 0

    entries = root.findall(f"{{{ATOM_NS}}}entry")
    logger.info(f"     {len(entries)} entrées dans le feed")

    scraped = 0
    for entry in entries:
        link_el = entry.find(f"{{{ATOM_NS}}}link")
        title_el = entry.find(f"{{{ATOM_NS}}}title")
        updated_el = entry.find(f"{{{ATOM_NS}}}updated")

        if title_el is None:
            continue

        offer_url = link_el.get("href", "") if link_el is not None else ""
        title = (title_el.text or "").strip()
        posted_date = (updated_el.text or "")[:10] or today_str()

        # Extraire le contenu texte de l'entrée
        content_el = entry.find(f".//{{{ATOM_NS}}}content")
        if content_el is None:
            content_el = entry.find(".//{http://www.w3.org/1999/xhtml}div")
        content_text = ""
        if content_el is not None:
            content_text = "".join(content_el.itertext())

        prog_id   = parse_campus_field(content_text, "Program ID")
        city      = parse_campus_field(content_text, "City")
        country_raw = parse_campus_field(content_text, "Program country")
        region_raw  = parse_campus_field(content_text, "Program region")
        description = parse_campus_field(content_text, "Program description")
        entry_level = parse_campus_field(content_text, "Entry level")
        lob         = parse_campus_field(content_text, "Program LOB")
        sub_lob     = parse_campus_field(content_text, "Program Sub LOB")
        prog_type   = parse_campus_field(content_text, "Program type")

        # Pays / région
        country_key = country_raw.lower().strip()
        country_fr, region = CAMPUS_COUNTRY_MAP.get(country_key, ("", ""))
        if not country_fr:
            # Fallback via ville
            country_fr, region = resolve_country(None, city)
        if not region or region == "Non spécifié":
            region = CAMPUS_REGION_MAP.get(region_raw.lower().strip(), region_raw or "Non spécifié")

        # Si toujours vide → essayer via titre (contient souvent la ville)
        if not country_fr:
            # Chercher un pays/ville dans le titre
            for city_key, (c, r) in CITY_COUNTRY_MAP.items():
                if city_key in title.lower():
                    country_fr, region = c, r
                    if not city:
                        city = city_key.title()
                    break
        if not country_fr:
            country_fr = country_raw or "Non spécifié"
            region = region or "Non spécifié"

        job_id = f"campus_{prog_id or hashlib.md5(offer_url.encode()).hexdigest()[:12]}"
        job_family = classify_family(f"{title} {lob} {sub_lob}")
        contract = classify_contract(prog_type, title, "campus")
        education = classify_education_campus(entry_level, description)
        exp_level = classify_level(title) if entry_level == "" else (
            "Junior (0-3 ans)" if entry_level.lower() in ("analyst", "intern", "internship")
            else "Intermédiaire (3-5 ans)" if "associate" in entry_level.lower()
            else "Junior (0-3 ans)"
        )

        _upsert_job(conn, {
            "job_id":          job_id,
            "job_title":       title,
            "city":            city,
            "country":         country_fr,
            "region":          region,
            "contract_type":   contract,
            "experience_level": exp_level,
            "education_level": education,
            "job_family":      job_family,
            "description":     description[:3000],
            "offer_url":       offer_url,
            "posted_date":     posted_date,
            "source":          "campus",
            "candidate_type":  "Campus",
            "division":        lob,
            "entity":          "Bank of America",
        })
        scraped += 1

    logger.info(f"     → {scraped} programmes campus")
    return scraped


# ──────────────────────────────── Base SQLite ────────────────────────────────
DDL = """
CREATE TABLE IF NOT EXISTS jobs (
    job_id           TEXT PRIMARY KEY,
    job_title        TEXT,
    city             TEXT,
    country          TEXT,
    region           TEXT,
    contract_type    TEXT,
    experience_level TEXT,
    education_level  TEXT,
    job_family       TEXT,
    description      TEXT,
    offer_url        TEXT,
    posted_date      TEXT,
    source           TEXT,
    candidate_type   TEXT,
    division         TEXT,
    entity           TEXT,
    scraped_at       TEXT DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_country  ON jobs(country);
CREATE INDEX IF NOT EXISTS idx_source   ON jobs(source);
CREATE INDEX IF NOT EXISTS idx_posted   ON jobs(posted_date);
"""


def init_db(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.executescript(DDL)
    conn.commit()
    return conn


def _upsert_job(conn: sqlite3.Connection, job: dict) -> None:
    conn.execute("""
        INSERT INTO jobs (
            job_id, job_title, city, country, region, contract_type,
            experience_level, education_level, job_family, description,
            offer_url, posted_date, source, candidate_type, division, entity, scraped_at
        ) VALUES (
            :job_id, :job_title, :city, :country, :region, :contract_type,
            :experience_level, :education_level, :job_family, :description,
            :offer_url, :posted_date, :source, :candidate_type, :division, :entity,
            datetime('now')
        )
        ON CONFLICT(job_id) DO UPDATE SET
            job_title        = excluded.job_title,
            city             = excluded.city,
            country          = excluded.country,
            region           = excluded.region,
            contract_type    = excluded.contract_type,
            experience_level = excluded.experience_level,
            education_level  = excluded.education_level,
            job_family       = excluded.job_family,
            description      = excluded.description,
            offer_url        = excluded.offer_url,
            posted_date      = excluded.posted_date,
            source           = excluded.source,
            candidate_type   = excluded.candidate_type,
            division         = excluded.division,
            entity           = excluded.entity,
            scraped_at       = datetime('now')
    """, job)
    conn.commit()


# ──────────────────────────────── Main ──────────────────────────────────────
def main() -> None:
    logger.info("=" * 70)
    logger.info("Bank of America — Scraper")
    logger.info("=" * 70)

    conn = init_db(DB_PATH)

    total = 0

    # 1. Workday sources (EMEA → APAC → US)
    for src in WORKDAY_SOURCES:
        try:
            n = scrape_workday_source(src, conn)
            total += n
        except Exception as e:
            logger.error(f"Erreur source {src['id']}: {e}", exc_info=True)

    # 2. Campus Atom feed
    try:
        n = scrape_campus_feed(conn)
        total += n
    except Exception as e:
        logger.error(f"Erreur campus feed: {e}", exc_info=True)

    # Résumé
    logger.info("=" * 70)
    logger.info(f"TOTAL : {total} offres upsertées")

    # Distribution finale
    cur = conn.execute("""
        SELECT country, region, COUNT(*) as n
        FROM jobs
        GROUP BY country, region
        ORDER BY n DESC
        LIMIT 30
    """)
    logger.info("Distribution pays :")
    non_spec = 0
    for row in cur.fetchall():
        c, r, n = row
        if not c or c == "Non spécifié":
            non_spec += n
        else:
            logger.info(f"  {n:5d}  {c:<30} ({r})")
    if non_spec:
        logger.warning(f"  {non_spec:5d}  Non spécifié (⚠️ à corriger)")

    conn.close()
    logger.info("Terminé.")


if __name__ == "__main__":
    main()
