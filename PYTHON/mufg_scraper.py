#!/usr/bin/env python3
"""
MUFG — JOB SCRAPER
==================
Source unique : API Workday (mufgub.wd3.myworkdayjobs.com/MUFG-Careers)
Couvre : US, UK, EMEA, APAC, Amériques (~580 offres)
Types : Experienced + Campus (Interns, Graduate Analysts)

Champs extraits :
  job_id, entity, candidate_type, job_title, job_family,
  city, country, region, contract_type, experience_level,
  education_level, description, offer_url, posted_date, scraped_at
"""

from __future__ import annotations

import html
import logging
import re
import sqlite3
import time
from datetime import datetime
from pathlib import Path
from typing import Optional

import requests
from bs4 import BeautifulSoup

try:
    from job_family_classifier import classify_job_family
    from experience_extractor import extract_experience_level
    from education_extractor import extract_education_level
except ImportError:
    import sys
    sys.path.append(str(Path(__file__).parent))
    from job_family_classifier import classify_job_family
    from experience_extractor import extract_experience_level
    from education_extractor import extract_education_level

# ─────────────────────────── Logging ────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("mufg_scraper.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger(__name__)

# ─────────────────────────── Config ─────────────────────────────
DB_PATH        = Path(__file__).parent / "mufg_jobs.db"
WD_API         = "https://mufgub.wd3.myworkdayjobs.com/wday/cxs/mufgub/MUFG-Careers/jobs"
WD_DETAIL_BASE = "https://mufgub.wd3.myworkdayjobs.com/wday/cxs/mufgub/MUFG-Careers/job"
WD_JOB_URL     = "https://mufgub.wd3.myworkdayjobs.com/fr-FR/MUFG-Careers/job"
REQUEST_DELAY  = 0.6
REQUEST_TIMEOUT = 25
MAX_RETRIES    = 3
PAGE_SIZE      = 20

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json,text/html,*/*",
    "Content-Type": "application/json",
}

# ═══════════════════════════════════════════════════════════════
# MAPPINGS
# ═══════════════════════════════════════════════════════════════

# Code ISO 2 lettres → (pays_fr, région)
COUNTRY_CODE_MAP: dict[str, tuple[str, str]] = {
    # Europe
    "GB": ("Royaume-Uni",          "Europe"),
    "DE": ("Allemagne",            "Europe"),
    "FR": ("France",               "Europe"),
    "NL": ("Pays-Bas",             "Europe"),
    "IT": ("Italie",               "Europe"),
    "ES": ("Espagne",              "Europe"),
    "CH": ("Suisse",               "Europe"),
    "IE": ("Irlande",              "Europe"),
    "LU": ("Luxembourg",           "Europe"),
    "BE": ("Belgique",             "Europe"),
    "SE": ("Suède",                "Europe"),
    "PL": ("Pologne",              "Europe"),
    "AT": ("Autriche",             "Europe"),
    "PT": ("Portugal",             "Europe"),
    # EMEA hors UE
    "TR": ("Turquie",              "Europe"),
    "AE": ("Émirats arabes unis",  "Europe"),
    "SA": ("Arabie Saoudite",      "Europe"),
    "ZA": ("Afrique du Sud",       "Europe"),
    # Amériques
    "US": ("États-Unis",           "Amérique du Nord"),
    "CA": ("Canada",               "Amérique du Nord"),
    "BR": ("Brésil",               "Amérique du Nord"),
    "MX": ("Mexique",              "Amérique du Nord"),
    # Asie-Pacifique
    "JP": ("Japon",                "Asie-Pacifique"),
    "HK": ("Hong Kong",            "Asie-Pacifique"),
    "SG": ("Singapour",            "Asie-Pacifique"),
    "AU": ("Australie",            "Asie-Pacifique"),
    "CN": ("Chine",                "Asie-Pacifique"),
    "IN": ("Inde",                 "Asie-Pacifique"),
    "KR": ("Corée du Sud",         "Asie-Pacifique"),
    "TW": ("Taïwan",               "Asie-Pacifique"),
    "MY": ("Malaisie",             "Asie-Pacifique"),
    "PH": ("Philippines",          "Asie-Pacifique"),
    "ID": ("Indonésie",            "Asie-Pacifique"),
    "TH": ("Thaïlande",            "Asie-Pacifique"),
    "VN": ("Vietnam",              "Asie-Pacifique"),
    "MM": ("Myanmar",              "Asie-Pacifique"),
    "NZ": ("Nouvelle-Zélande",     "Asie-Pacifique"),
}

# Pays en anglais (Workday) → ISO alpha2
COUNTRY_EN_MAP: dict[str, str] = {
    "united states of america": "US",
    "united states":            "US",
    "united kingdom":           "GB",
    "india":                    "IN",
    "singapore":                "SG",
    "hong kong":                "HK",
    "australia":                "AU",
    "japan":                    "JP",
    "germany":                  "DE",
    "france":                   "FR",
    "netherlands":              "NL",
    "italy":                    "IT",
    "spain":                    "ES",
    "switzerland":              "CH",
    "canada":                   "CA",
    "brazil":                   "BR",
    "mexico":                   "MX",
    "indonesia":                "ID",
    "malaysia":                 "MY",
    "philippines":              "PH",
    "korea, republic of":       "KR",
    "south korea":              "KR",
    "taiwan":                   "TW",
    "vietnam":                  "VN",
    "thailand":                 "TH",
    "myanmar":                  "MM",
    "turkey":                   "TR",
    "türkiye":                  "TR",
    "china":                    "CN",
}

# Ville → pays ISO2 (fallback si country absent)
CITY_COUNTRY_MAP: dict[str, str] = {
    "london":      "GB", "paris":      "FR", "frankfurt":  "DE", "amsterdam":  "NL",
    "milan":       "IT", "zurich":     "CH", "madrid":     "ES", "amsterdam":  "NL",
    "new york":    "US", "jersey city": "US", "tempe":     "US", "tampa":      "US",
    "chicago":     "US", "los angeles": "US", "san francisco": "US", "boston":  "US",
    "charlotte":   "US", "washington":  "US", "nashville": "US", "irving":     "US",
    "menlo park":  "US", "walnut creek":"US",
    "toronto":     "CA",
    "sao paulo":   "BR", "são paulo":  "BR",
    "mexico city": "MX",
    "tokyo":       "JP", "osaka":      "JP",
    "hong kong":   "HK",
    "singapore":   "SG",
    "sydney":      "AU", "melbourne":  "AU", "perth":      "AU",
    "seoul":       "KR",
    "taipei":      "TW",
    "jakarta":     "ID",
    "kuala lumpur": "MY",
    "manila":      "PH",
    "hanoi":       "VN",
    "yangon":      "MM",
    "mumbai":      "IN", "bengaluru":  "IN", "bangalore":  "IN",
    "new delhi":   "IN", "delhi":      "IN", "chennai":    "IN",
    "istanbul":    "TR",
}

# Familles métier MUFG (anglais Workday) → libellés canoniques français
FAMILY_MAP: dict[str, str] = {
    "technology":                       "IT, Digital et Data",
    "data analytics":                   "IT, Digital et Data",
    "corporate and investment banking": "Financement et Investissement",
    "cib":                              "Financement et Investissement",
    "front office":                     "Marchés financiers / Sales & Trading",
    "sales & trading":                  "Marchés financiers / Sales & Trading",
    "credit":                           "Financement et Investissement",
    "transaction banking":              "Financement et Investissement",
    "commercial banking":               "Commercial / Relations Clients",
    "operations":                       "Gestion des opérations",
    "risk":                             "Risques / Contrôles permanents",
    "risk & issue management":          "Risques / Contrôles permanents",
    "financial crimes":                 "Conformité / Sécurité financière",
    "compliance":                       "Conformité / Sécurité financière",
    "finance":                          "Finances / Comptabilité / Contrôle de gestion",
    "audit":                            "Inspection / Audit",
    "legal":                            "Juridique",
    "human resources":                  "Ressources Humaines",
    "treasury":                         "Finances / Comptabilité / Contrôle de gestion",
    "marketing":                        "Marketing et Communication",
    "corporate relations":              "Marketing et Communication",
    "corporate and general services":   "Direction générale",
    "project management":               "Direction générale",
    "corporate & business services":    "Direction générale",
    "incentive plans":                  "Ressources Humaines",
    "customer and client services":     "Commercial / Relations Clients",
}

# Grades MUFG Workday (Corporate Title) → tranches d'années
LEVEL_PATTERNS: list[tuple[str, str]] = [
    (r"\bManaging\s+Director\b|\bMD\b(?!\s+Anderson|\s+degree)",  "11 ans et plus"),
    (r"\bExecutive\s+Director\b|\bED\b",                          "11 ans et plus"),
    (r"\bDirector\b(?!\s+of\s+talent|\s+General|\s+Officer)",     "11 ans et plus"),
    (r"\bHead\s+of\b",                                            "11 ans et plus"),
    (r"\bSenior\s+Vice\s+President\b|\bSVP\b",                   "11 ans et plus"),
    (r"\bVice\s+President\b|\bVP\b",                              "6 - 10 ans"),
    (r"\bAssistant\s+Vice\s+President\b|\bAVP\b",                 "3 - 5 ans"),
    (r"\bSenior\s+(Manager|Analyst|Associate)\b",                 "6 - 10 ans"),
    (r"\bManager\b",                                              "6 - 10 ans"),
    (r"\bAssociate\b",                                            "3 - 5 ans"),
    (r"\bAnalyst\b",                                              "0 - 2 ans"),
    (r"\bAdministrator\b",                                        "0 - 2 ans"),
    (r"\bIntern(?:ship)?\b|\bSummer\s+Analyst\b|\bGraduate\s+Analyst\b", "0 - 2 ans"),
]

# Entités MUFG → nom normalisé
ENTITY_PATTERNS: list[tuple[str, str]] = [
    (r"MUFG\s+Securities",       "MUFG Securities"),
    (r"MUFG\s+Global\s+Service", "MUFG"),
    (r"MUFG\s+Americas",         "MUFG"),
    (r"MUFG\s+Bank",             "MUFG Bank"),
    (r"MUFG",                    "MUFG"),
]

# Patterns Campus (candidate_type = "Campus")
CAMPUS_PATTERNS = re.compile(
    r"\bIntern(?:ship)?\b|\bSummer\s+Analyst\b|\bGraduate\s+(Analyst|Programme|Program)\b"
    r"|\bOff[\s-]Cycle\b|\bNew\s+Graduate\b|\bEntry[\s-]Level\b|\bGraduate\s+Recruit",
    re.IGNORECASE,
)


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
CREATE INDEX IF NOT EXISTS idx_region   ON jobs(region);
CREATE INDEX IF NOT EXISTS idx_contract ON jobs(contract_type);
CREATE INDEX IF NOT EXISTS idx_entity   ON jobs(entity);
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
    ph   = ", ".join(["?"] * len(job))
    conn.execute(f"INSERT OR REPLACE INTO jobs ({cols}) VALUES ({ph})", list(job.values()))
    conn.commit()


# ═══════════════════════════════════════════════════════════════
# HTTP
# ═══════════════════════════════════════════════════════════════

SESSION = requests.Session()
SESSION.headers.update(HEADERS)


def fetch_json(url: str, method: str = "GET", payload: Optional[dict] = None) -> Optional[dict]:
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            if method == "POST":
                r = SESSION.post(url, json=payload, timeout=REQUEST_TIMEOUT)
            else:
                r = SESSION.get(url, timeout=REQUEST_TIMEOUT)
            if r.status_code == 200:
                return r.json()
            if r.status_code in (403, 404, 410):
                logger.warning(f"HTTP {r.status_code} → {url}")
                return None
            logger.warning(f"HTTP {r.status_code} attempt {attempt}/{MAX_RETRIES}")
        except Exception as e:
            logger.warning(f"Erreur réseau attempt {attempt}/{MAX_RETRIES} : {e}")
        if attempt < MAX_RETRIES:
            time.sleep(2 ** attempt)
    return None


# ═══════════════════════════════════════════════════════════════
# NORMALISATION
# ═══════════════════════════════════════════════════════════════

def normalize_country_region(
    alpha2: Optional[str],
    country_en: Optional[str] = None,
    city: Optional[str] = None,
) -> tuple[str, str]:
    """Retourne (pays_fr, région) par ordre de priorité : alpha2 > country_en > city."""
    if alpha2:
        code = alpha2.strip().upper()
        if code in COUNTRY_CODE_MAP:
            return COUNTRY_CODE_MAP[code]
    if country_en:
        code = COUNTRY_EN_MAP.get(country_en.strip().lower())
        if code and code in COUNTRY_CODE_MAP:
            return COUNTRY_CODE_MAP[code]
    if city:
        c = city.strip().lower()
        # Correspondance exacte d'abord
        if c in CITY_COUNTRY_MAP:
            code = CITY_COUNTRY_MAP[c]
            return COUNTRY_CODE_MAP.get(code, ("Non spécifié", "Non spécifié"))
        # Correspondance partielle
        for key, code in CITY_COUNTRY_MAP.items():
            if key in c or c in key:
                return COUNTRY_CODE_MAP.get(code, ("Non spécifié", "Non spécifié"))
    return ("Non spécifié", "Non spécifié")


def extract_city_from_location(location_text: str) -> str:
    """
    Extrait la ville depuis le texte de localisation Workday.
    Ex: "MUFG Global Service Private Ltd. - Bengaluru (BCIT)" → "Bengaluru"
        "New York, NY" → "New York"
        "Jersey City, NJ" → "Jersey City"
        "2 Locations" → ""
    """
    if not location_text or location_text == "2 Locations":
        return ""
    loc = location_text.strip()
    # Cas "City, State" (US)
    m = re.match(r'^([^,]+),\s*[A-Z]{2}$', loc)
    if m:
        return m.group(1).strip()
    # Cas "Entity - City (suffix)" → prendre avant la parenthèse après tiret
    m = re.search(r'-\s*([A-Z][a-z][^-(]+?)(?:\s*\(|$)', loc)
    if m:
        candidate = m.group(1).strip()
        # Vérifier que ce n'est pas un descriptor de bureau (ex: "Bengaluru (BCIT)")
        if len(candidate) > 2 and not re.search(r'\b(Ltd|Inc|Corp|Branch|Office|Plc)\b', candidate):
            return candidate
    # Cas branche directe "London", "Singapore Office Marina One" → extraire premier mot
    if 'branch' in loc.lower() or 'office' in loc.lower():
        m2 = re.match(r'^([A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)', loc)
        if m2:
            return m2.group(1)
    return loc if len(loc) < 40 else ""


def normalize_entity(hiring_org_name: str) -> str:
    if not hiring_org_name:
        return "MUFG"
    for pattern, name in ENTITY_PATTERNS:
        if re.search(pattern, hiring_org_name, re.IGNORECASE):
            return name
    return "MUFG"


def extract_level(title: str, description: str = "") -> str:
    """Extrait le niveau d'expérience depuis le titre (puis la description)."""
    for pattern, level in LEVEL_PATTERNS:
        if re.search(pattern, title, re.IGNORECASE):
            return level
    # Fallback description
    lv = extract_experience_level(description[:2000], "CDI", title)
    return lv or "Non spécifié"


def normalize_contract(title: str, time_type: str = "") -> str:
    """Déduit le type de contrat."""
    combined = f"{title} {time_type}".lower()
    if re.search(r'\bintern(?:ship)?\b|\bstage\b|\boff[\s-]cycle\b|\bsummer\s+analyst\b', combined):
        return "Stage"
    if re.search(r'\balternance\b|\bapprentice\b|\bworking\s+student\b', combined):
        return "Alternance"
    if re.search(r'\bcontract(?:or|ed)?\b|\bfixed[\s-]term\b|\btemporary\b|\bcdd\b|\bftc\b', combined):
        return "CDD"
    if re.search(r'\bpart[\s-]time\b', combined):
        return "CDI"  # temps partiel → CDI à temps partiel
    if re.search(r'\bvie\b', combined):
        return "VIE"
    return "CDI"


def classify_family(title: str, description: str, workday_category: str = "") -> str:
    """Classifie la famille métier en libellé canonique français."""
    # Chercher dans le mapping Workday d'abord
    if workday_category:
        cat_lower = workday_category.strip().lower()
        for key, val in FAMILY_MAP.items():
            if key in cat_lower:
                return val
    # Fallback classifieur générique
    result = classify_job_family(f"{title} {description[:200]}")
    return result or "Autres"


def clean_description(raw: str) -> str:
    """Nettoie le HTML de la description Workday."""
    if not raw:
        return ""
    # Décoder les entités HTML
    raw = html.unescape(raw)
    # Supprimer les balises HTML
    soup = BeautifulSoup(raw, 'lxml')
    text = soup.get_text(separator=' ')
    # Normaliser les espaces
    text = re.sub(r'\s+', ' ', text).strip()
    return text[:4000]


# ═══════════════════════════════════════════════════════════════
# WORKDAY API
# ═══════════════════════════════════════════════════════════════

def fetch_all_listings() -> list[dict]:
    """Récupère toutes les offres depuis l'API Workday (pagination 100/page)."""
    all_jobs = []
    offset = 0

    # Premier appel pour connaître le total
    payload = {"limit": PAGE_SIZE, "offset": 0, "searchText": "", "appliedFacets": {}}
    first = fetch_json(WD_API, "POST", payload)
    if not first:
        logger.error("Impossible de contacter l'API Workday MUFG.")
        return []

    total = first.get("total", 0)
    logger.info(f"Total offres Workday MUFG : {total}")
    all_jobs.extend(first.get("jobPostings") or [])
    offset = PAGE_SIZE

    while offset < total:
        payload["offset"] = offset
        data = fetch_json(WD_API, "POST", payload)
        if not data:
            break
        batch = data.get("jobPostings") or []
        if not batch:
            break
        all_jobs.extend(batch)
        offset += PAGE_SIZE
        logger.debug(f"Listing page : {offset}/{total}")
        time.sleep(REQUEST_DELAY)

    logger.info(f"Listing complet : {len(all_jobs)} offres récupérées.")
    return all_jobs


def fetch_detail(external_path: str) -> Optional[dict]:
    """Récupère les détails d'une offre (description, pays, entité…)."""
    # external_path = "/job/London/Job-Title_10076860-WD"
    path = external_path.lstrip('/')
    url = f"{WD_DETAIL_BASE}/{'/'.join(path.split('/')[1:])}"  # retire le "job/" initial
    data = fetch_json(url)
    if not data:
        return None
    return data.get("jobPostingInfo") or data


# ═══════════════════════════════════════════════════════════════
# PARSE ET STOCKAGE
# ═══════════════════════════════════════════════════════════════

def parse_job(listing: dict, detail: Optional[dict]) -> dict:
    """
    Combine listing + detail en un enregistrement normalisé.
    detail peut être None si le fetch a échoué.
    """
    title     = listing.get("title") or (detail or {}).get("title") or "Non spécifié"
    location  = listing.get("locationsText") or ""
    posted_on = listing.get("postedOn") or ""
    # Extraire la date depuis "Posted Today" / "Posted X Days Ago" / ISO date
    if detail:
        start = detail.get("startDate") or ""
        pub_date = start[:10] if start else re.sub(r'\D', '', datetime.utcnow().isoformat()[:10])
    else:
        pub_date = datetime.utcnow().isoformat()[:10]

    # ── Localisation ─────────────────────────────────────────────
    alpha2 = None
    country_en = None
    if detail:
        req_loc = detail.get("jobRequisitionLocation") or {}
        country_obj = req_loc.get("country") or detail.get("country") or {}
        if isinstance(country_obj, dict):
            alpha2     = country_obj.get("alpha2Code")
            country_en = country_obj.get("descriptor")
        location = req_loc.get("descriptor") or detail.get("location") or location

    city   = extract_city_from_location(location)
    country, region = normalize_country_region(alpha2, country_en, city or location)

    # ── Entité ───────────────────────────────────────────────────
    hiring_org = ""
    if detail:
        ho = {}  # hiringOrganization n'est pas dans jobPostingInfo, c'est dans la réponse racine
    entity = "MUFG"  # fallback; sera écrasé si on a l'info

    # ── Description ──────────────────────────────────────────────
    description = ""
    if detail:
        raw_desc = detail.get("jobDescription") or ""
        description = clean_description(raw_desc)

    # ── Type de candidat ─────────────────────────────────────────
    candidate_type = "Campus" if CAMPUS_PATTERNS.search(title) else "Experienced"

    # ── Contrat ───────────────────────────────────────────────────
    time_type = (detail or {}).get("timeType") or listing.get("timeType") or ""
    contract_type = normalize_contract(title, time_type)

    # ── Niveau d'expérience ───────────────────────────────────────
    experience_level = extract_level(title, description)

    # ── Niveau d'études ───────────────────────────────────────────
    education_level = extract_education_level(description, contract_type, title) or "Non spécifié"

    # ── Famille métier ────────────────────────────────────────────
    job_family = classify_family(title, description)

    # ── Job ID + URL ──────────────────────────────────────────────
    ext_path = listing.get("externalPath") or ""
    bullet   = (listing.get("bulletFields") or [""])[0]
    job_req  = bullet or re.search(r'_(\d{8,}-WD)', ext_path or "")
    job_id   = job_req.group(1) if hasattr(job_req, 'group') else (bullet or ext_path[-20:])
    job_url  = f"{WD_JOB_URL}/{ext_path.lstrip('/job/')}" if ext_path else ""

    return {
        "job_id":           f"mufg_{job_id}",
        "source":           "mufgub.wd3.myworkdayjobs.com",
        "candidate_type":   candidate_type,
        "entity":           entity,
        "job_title":        title,
        "job_family":       job_family,
        "city":             city or "Non spécifié",
        "country":          country,
        "region":           region,
        "contract_type":    contract_type,
        "experience_level": experience_level,
        "education_level":  education_level,
        "description":      description[:4000],
        "offer_url":        job_url,
        "posted_date":      pub_date,
        "scraped_at":       datetime.utcnow().isoformat(),
    }


def scrape(conn: sqlite3.Connection) -> int:
    """Pipeline principal : listing → détails → stockage."""
    existing = get_existing_ids(conn)
    listings = fetch_all_listings()
    if not listings:
        return 0

    saved = 0
    total = len(listings)

    for i, listing in enumerate(listings, 1):
        ext_path = listing.get("externalPath") or ""
        bullet   = (listing.get("bulletFields") or [""])[0]
        m        = re.search(r'_(\d{8,}-WD)', ext_path)
        job_req  = m.group(1) if m else bullet
        uid      = f"mufg_{job_req}"

        if uid in existing:
            logger.debug(f"[{i}/{total}] Déjà en base : {uid}")
            continue

        logger.info(f"[{i}/{total}] {listing.get('title','?')[:60]} | {listing.get('locationsText','')}")

        # Fetch detail
        detail = None
        if ext_path:
            # Construire l'URL correcte : retirer le préfixe "/job/"
            path_parts = ext_path.strip('/').split('/')
            if path_parts[0] == 'job':
                path_parts = path_parts[1:]
            detail_url = f"{WD_DETAIL_BASE}/{'/'.join(path_parts)}"
            data = fetch_json(detail_url)
            if data:
                detail = data.get("jobPostingInfo") or data
                # Récupérer l'entité depuis hiringOrganization (niveau racine)
                ho = data.get("hiringOrganization") or {}
                if isinstance(ho, dict) and ho.get("name"):
                    listing["_entity"] = normalize_entity(ho["name"])

        # Appliquer l'entité depuis hiringOrganization si disponible
        job = parse_job(listing, detail)
        if listing.get("_entity"):
            job["entity"] = listing["_entity"]

        upsert_job(conn, job)
        existing.add(uid)
        saved += 1
        logger.info(
            f"  ✓ {job['job_title'][:55]:<55} | {job['city']}, {job['country']:<20}"
            f" | {job['contract_type']:<10} | {job['experience_level']}"
        )
        time.sleep(REQUEST_DELAY)

    logger.info(f"Scraping terminé : {saved} nouvelles offres enregistrées.")
    return saved


# ═══════════════════════════════════════════════════════════════
# STATISTIQUES + EXPORT CSV
# ═══════════════════════════════════════════════════════════════

def print_stats(conn: sqlite3.Connection) -> None:
    total = conn.execute("SELECT COUNT(*) FROM jobs").fetchone()[0]
    logger.info(f"\n{'='*60}\nTOTAL : {total} offres MUFG\n")

    for label, query in [
        ("── Par région ──",        "SELECT region, COUNT(*) n FROM jobs GROUP BY region ORDER BY n DESC"),
        ("── Par type de candidat ──", "SELECT candidate_type, COUNT(*) n FROM jobs GROUP BY candidate_type ORDER BY n DESC"),
        ("── Par contrat ──",       "SELECT contract_type, COUNT(*) n FROM jobs GROUP BY contract_type ORDER BY n DESC"),
        ("── Par entité ──",        "SELECT entity, COUNT(*) n FROM jobs GROUP BY entity ORDER BY n DESC"),
        ("── Par niveau ──",        "SELECT experience_level, COUNT(*) n FROM jobs GROUP BY experience_level ORDER BY n DESC"),
    ]:
        logger.info(f"\n{label}")
        for row in conn.execute(query):
            logger.info(f"  {row[0]:<30} {row[1]:>4}")

    logger.info("\n── Données manquantes ──")
    for col in ('city', 'country', 'region', 'contract_type', 'experience_level', 'job_family'):
        n = conn.execute(f"SELECT COUNT(*) FROM jobs WHERE {col} IS NULL OR {col}='Non spécifié'").fetchone()[0]
        icon = "✓ " if n == 0 else "⚠️ "
        pct = f"({n*100//total}%)" if n else ""
        logger.info(f"  {icon}{col:<22} : {n} / {total} {pct}")

    logger.info('='*60)


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
# POINT D'ENTRÉE
# ═══════════════════════════════════════════════════════════════

def main() -> None:
    import argparse
    parser = argparse.ArgumentParser(description="MUFG Job Scraper — Workday API")
    parser.add_argument('--export-csv',  action='store_true', help='Exporter CSV après scraping')
    parser.add_argument('--stats-only',  action='store_true', help='Afficher stats sans scraper')
    parser.add_argument('--db', default=str(DB_PATH), help='Chemin SQLite')
    args = parser.parse_args()

    db_path = Path(args.db)
    conn    = init_db(db_path)
    logger.info(f"Base de données : {db_path}")

    if args.stats_only:
        print_stats(conn)
        conn.close()
        return

    count = scrape(conn)
    logger.info(f"\n✅ {count} nouvelles offres MUFG enregistrées")
    print_stats(conn)

    if args.export_csv:
        export_csv(conn, db_path.with_suffix('.csv'))

    conn.close()


if __name__ == '__main__':
    main()
