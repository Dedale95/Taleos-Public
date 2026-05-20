#!/usr/bin/env python3
"""
HSBC — JOB SCRAPER
===================
Source principale : portail Eightfold AI (portal.careers.hsbc.com)
  → API REST sans auth → ~1 600 offres dans tous les pays

Phase 1 : API liste paginée  (/api/apply/v2/jobs?domain=hsbc.com)
            → collecte title, location, department, ats_job_id, url
Phase 2 : API détail         (/api/apply/v2/jobs/{id}?domain=hsbc.com)
            → description complète, jobType (CDD/CDI/Stage…), workStyle, dates

Couverture : France, UK, Allemagne, Hong Kong, Inde, Singapour,
             Luxembourg, UAE, USA, Australie, Pologne, Brésil, etc.
             + Hang Seng Bank + HSBC Innovation Banking
"""

import json
import logging
import re
import sqlite3
import time
from datetime import datetime, timezone
from html import unescape
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

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

COMPANY_NAME  = "HSBC"
DB_PATH       = Path(__file__).parent / "hsbc_jobs.db"

# Eightfold AI API
API_BASE      = "https://portal.careers.hsbc.com/api/apply/v2/jobs"
DOMAIN        = "hsbc.com"
API_HL        = "en"
PAGE_SIZE     = 10       # max stable Eightfold (on teste 25 en fallback)
DETAIL_DELAY  = 0.4      # secondes entre appels API détail
LIST_DELAY    = 0.3      # secondes entre appels API liste
REQUEST_TIMEOUT = 20

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept":          "application/json, text/javascript, */*; q=0.01",
    "Accept-Language": "en-GB,en;q=0.9,fr;q=0.8",
    "Referer":         "https://portal.careers.hsbc.com/careers",
}

# ─────────────────────────── Logging ────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════
# CONTRAT  —  mapping Eightfold jobType + titre
# ═══════════════════════════════════════════════════════════════

_KNOWN_CT = {
    "CDI", "CDD", "Stage", "Alternance / Apprentissage",
    "V.I.E.", "Intérim", "Temps partiel", "Indépendant / Entrepreneur",
}

def parse_contract_type(job_type_raw: str, title: str) -> str:
    """
    Mappe le champ Eightfold 'jobType' + le titre vers nos types standard.
    Priorité : mots-clés dans le titre > champ jobType.
    """
    t  = (title or "").lower()
    jt = (job_type_raw or "").lower()

    # ── Inférence depuis le titre (haute confiance) ─────────────
    if re.search(r"\bstage\b|\bstagiaire\b|\binternship\b|\bstagiai\b"
                 r"|\bpraktikum\b|\btirocinio\b", t):
        return "Stage"
    if re.search(r"\balternance\b|\balternant[e]?\b|\bapprentice\b"
                 r"|\bapprenti[e]?\b|\bausbildung\b|\bwerkstudent\b", t):
        return "Alternance / Apprentissage"
    if re.search(r"\bv\.?i\.?e\.?\b|\bvolontariat\s+international\b", t):
        return "V.I.E."
    if re.search(r"\bgraduate\s+programme?\b|\bgraduate\s+program\b", t):
        return "CDD"   # Programme diplômant à durée déterminée

    # ── Depuis le champ jobType Eightfold ───────────────────────
    if "fixed term" in jt:
        return "CDD"
    if "part time" in jt and "fixed" not in jt:
        return "Temps partiel"

    # ── Défaut : full time permanent ────────────────────────────
    return "CDI"


# ═══════════════════════════════════════════════════════════════
# LOCALISATION
# ═══════════════════════════════════════════════════════════════

def parse_location(location_str: str) -> Tuple[str, str, str]:
    """
    Retourne (location_display, country_normalized, region_normalized).
    Location Eightfold : "London, United Kingdom" ou "PARIS, Paris, France"
    """
    if not location_str:
        return "", "", ""

    # Nettoyage (parfois en majuscules ex: "PARIS, Paris, France")
    parts = [p.strip() for p in location_str.split(",")]

    # Dernier token = pays en anglais généralement
    country_raw = parts[-1] if parts else ""
    city_raw    = parts[0].title() if parts else ""

    country_normalized = normalize_country(country_raw) or normalize_country(
        get_country_from_city(city_raw) or ""
    )

    # Ville + pays en affichage propre
    if len(parts) >= 2:
        display = f"{city_raw} - {country_normalized or country_raw}"
    else:
        display = country_normalized or location_str

    # Région : ville (approximation)
    region = city_raw

    return display, country_normalized or country_raw, region


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
                    company_name         TEXT,
                    brand                TEXT,
                    source               TEXT DEFAULT 'HSBC',
                    eightfold_id         TEXT,
                    ats_job_id           TEXT,
                    is_valid             INTEGER DEFAULT 1,
                    first_seen           TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    last_updated         TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            conn.execute("CREATE INDEX IF NOT EXISTS idx_eightfold ON jobs(eightfold_id)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_status    ON jobs(status)")
            conn.commit()
        self._migrate()

    def _migrate(self):
        """Supprime le UNIQUE constraint sur eightfold_id s'il existe (migration DB existante)."""
        with sqlite3.connect(self.db_path) as conn:
            indices = conn.execute("PRAGMA index_list(jobs)").fetchall()
            ef_unique_index = None
            for idx in indices:
                idx_name, idx_unique = idx[1], idx[2]
                if idx_unique:
                    cols = conn.execute(f"PRAGMA index_info('{idx_name}')").fetchall()
                    if any(col[2] == 'eightfold_id' for col in cols):
                        ef_unique_index = idx_name
                        break
            if ef_unique_index is None:
                return
            logger.info(f"Migration DB HSBC : suppression index UNIQUE sur eightfold_id ({ef_unique_index})")
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
                    company_name         TEXT,
                    brand                TEXT,
                    source               TEXT DEFAULT 'HSBC',
                    eightfold_id         TEXT,
                    ats_job_id           TEXT,
                    is_valid             INTEGER DEFAULT 1,
                    first_seen           TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    last_updated         TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            conn.execute("INSERT INTO jobs SELECT * FROM _jobs_bak")
            conn.execute("DROP TABLE _jobs_bak")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_eightfold ON jobs(eightfold_id)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_status    ON jobs(status)")
            conn.commit()
            logger.info("Migration DB HSBC terminée")

    def upsert(self, job: Dict) -> bool:
        """Insert ou met à jour, retourne True si c'est une nouvelle offre."""
        url = job.get("job_url", "")
        if not url:
            return False
        with sqlite3.connect(self.db_path) as conn:
            existing = conn.execute(
                "SELECT job_url FROM jobs WHERE job_url = ?", (url,)
            ).fetchone()
            is_new = existing is None
            # Neutraliser eightfold_id si déjà pris par un autre job_url
            eightfold_id = job.get("eightfold_id", "") or ""
            if eightfold_id:
                conflict = conn.execute(
                    "SELECT job_url FROM jobs WHERE eightfold_id = ? AND job_url != ?",
                    (eightfold_id, url)
                ).fetchone()
                if conflict:
                    eightfold_id = None
            params = {
                "job_url":           job.get("job_url", ""),
                "job_id":            job.get("job_id", ""),
                "job_title":         job.get("job_title", ""),
                "contract_type":     job.get("contract_type", ""),
                "publication_date":  job.get("publication_date", ""),
                "location":          job.get("location", ""),
                "country":           job.get("country", ""),
                "region":            job.get("region", ""),
                "job_family":        job.get("job_family", ""),
                "work_style":        job.get("work_style", ""),
                "management_position": job.get("management_position", "Non"),
                "status":            job.get("status", "Live"),
                "education_level":   job.get("education_level", ""),
                "experience_level":  job.get("experience_level", ""),
                "job_description":   job.get("job_description", ""),
                "company_name":      job.get("company_name", COMPANY_NAME),
                "brand":             job.get("brand", "HSBC"),
                "source":            "HSBC",
                "eightfold_id":      eightfold_id,
                "ats_job_id":        job.get("ats_job_id", ""),
                "is_valid":          job.get("is_valid", 1),
            }
            try:
                conn.execute("""
                    INSERT INTO jobs (
                        job_url, job_id, job_title, contract_type, publication_date,
                        location, country, region, job_family, work_style,
                        management_position, status, education_level, experience_level,
                        job_description, company_name, brand, source,
                        eightfold_id, ats_job_id, is_valid, last_updated
                    ) VALUES (
                        :job_url, :job_id, :job_title, :contract_type, :publication_date,
                        :location, :country, :region, :job_family, :work_style,
                        :management_position, :status, :education_level, :experience_level,
                        :job_description, :company_name, :brand, :source,
                        :eightfold_id, :ats_job_id, :is_valid, CURRENT_TIMESTAMP
                    )
                    ON CONFLICT(job_url) DO UPDATE SET
                        job_title        = excluded.job_title,
                        contract_type    = CASE WHEN excluded.contract_type != ''
                                               THEN excluded.contract_type
                                               ELSE jobs.contract_type END,
                        location         = excluded.location,
                        country          = excluded.country,
                        region           = excluded.region,
                        job_family       = CASE WHEN excluded.job_family != ''
                                               THEN excluded.job_family
                                               ELSE jobs.job_family END,
                        work_style       = excluded.work_style,
                        status           = excluded.status,
                        education_level  = CASE WHEN excluded.education_level != ''
                                               THEN excluded.education_level
                                               ELSE jobs.education_level END,
                        experience_level = CASE WHEN excluded.experience_level != ''
                                               THEN excluded.experience_level
                                               ELSE jobs.experience_level END,
                        job_description  = CASE WHEN excluded.job_description != ''
                                               THEN excluded.job_description
                                               ELSE jobs.job_description END,
                        brand            = excluded.brand,
                        last_updated     = CURRENT_TIMESTAMP,
                        is_valid         = excluded.is_valid
                """, params)
                conn.commit()
            except sqlite3.IntegrityError as e:
                if "eightfold_id" in str(e).lower():
                    logger.warning(f"eightfold_id conflict ignoré pour {url}: {e}")
                else:
                    raise
        return is_new

    def get_live_eightfold_ids(self) -> Set[str]:
        with sqlite3.connect(self.db_path) as conn:
            rows = conn.execute(
                "SELECT eightfold_id FROM jobs WHERE status='Live' AND eightfold_id IS NOT NULL"
            ).fetchall()
        return {r[0] for r in rows if r[0]}

    def get_urls_without_description(self) -> Set[str]:
        with sqlite3.connect(self.db_path) as conn:
            rows = conn.execute(
                "SELECT job_url FROM jobs WHERE status='Live' "
                "AND (job_description IS NULL OR job_description = '')"
            ).fetchall()
        return {r[0] for r in rows}

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

    def count_live(self) -> int:
        with sqlite3.connect(self.db_path) as conn:
            return conn.execute(
                "SELECT COUNT(*) FROM jobs WHERE status='Live'"
            ).fetchone()[0]

    def backfill_education_experience(self):
        """Ré-infère éducation + expérience pour les offres sans valeur."""
        with sqlite3.connect(self.db_path) as conn:
            rows = conn.execute(
                "SELECT job_url, job_title, job_description, contract_type "
                "FROM jobs WHERE status='Live' AND ("
                "education_level IS NULL OR education_level = '' OR "
                "experience_level IS NULL OR experience_level = '')"
            ).fetchall()
            updated = 0
            for url, title, desc, ct in rows:
                edu = extract_education_level(desc or "", ct or "", title or "")
                exp = extract_experience_level(desc or "", ct or "", title or "")
                if edu or exp:
                    conn.execute(
                        "UPDATE jobs SET "
                        "education_level  = COALESCE(NULLIF(education_level,''),  ?), "
                        "experience_level = COALESCE(NULLIF(experience_level,''), ?), "
                        "last_updated = CURRENT_TIMESTAMP WHERE job_url=?",
                        (edu or None, exp or None, url),
                    )
                    updated += 1
            conn.commit()
        return updated


# ═══════════════════════════════════════════════════════════════
# PHASE 1 — API LISTE
# ═══════════════════════════════════════════════════════════════

def _api_list(session: requests.Session, start: int, num: int = PAGE_SIZE) -> Dict:
    """Appelle l'API Eightfold liste et retourne le JSON brut."""
    params = {
        "domain": DOMAIN,
        "hl":     API_HL,
        "start":  start,
        "num":    num,
        "query":  "",
        "location": "",
    }
    resp = session.get(API_BASE, params=params, headers=HEADERS,
                       timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()
    return resp.json()


def fetch_all_jobs(session: requests.Session) -> List[Dict]:
    """
    Collecte toutes les offres via l'API liste paginée.
    Retourne la liste brute des positions Eightfold.
    """
    # Première page pour connaître le total
    try:
        first = _api_list(session, start=0, num=1)
    except Exception as e:
        logger.error(f"Phase 1 — première requête échouée : {e}")
        return []

    total = first.get("count", 0)
    if not total:
        logger.error("Phase 1 — count=0, impossible de continuer")
        return []

    logger.info(f"Phase 1 — {total} offres à récupérer")

    # Vérifier si num > PAGE_SIZE est accepté (réduit les appels)
    try:
        test25 = _api_list(session, start=0, num=25)
        effective_size = len(test25.get("positions", [])) or PAGE_SIZE
        if effective_size >= 25:
            page_size = 25
            logger.info("Phase 1 — utilisation de num=25 par page")
        else:
            page_size = PAGE_SIZE
    except Exception:
        page_size = PAGE_SIZE

    all_positions: List[Dict] = []
    start = 0
    while start < total:
        try:
            data = _api_list(session, start=start, num=page_size)
            positions = data.get("positions", [])
            if not positions:
                break
            all_positions.extend(positions)
            logger.info(f"  {len(all_positions)}/{total} offres collectées")
            start += len(positions)
        except requests.HTTPError as e:
            logger.warning(f"Phase 1 — erreur HTTP start={start} : {e}")
            start += page_size
        except Exception as e:
            logger.warning(f"Phase 1 — erreur start={start} : {e}")
            start += page_size
        time.sleep(LIST_DELAY)

    return all_positions


# ═══════════════════════════════════════════════════════════════
# PHASE 2 — API DÉTAIL
# ═══════════════════════════════════════════════════════════════

def _html_to_text(html: str) -> str:
    """Convertit le HTML de la description en texte propre."""
    if not html:
        return ""
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text(separator=" ", strip=True)
    text = re.sub(r"\s+", " ", text).strip()
    return text[:25_000]


def fetch_detail(session: requests.Session, eightfold_id: str) -> Optional[Dict]:
    """
    Appelle l'API détail Eightfold pour un job et retourne les champs enrichis.
    Retourne None en cas d'erreur.
    """
    url = f"{API_BASE}/{eightfold_id}"
    params = {"domain": DOMAIN, "hl": API_HL}
    try:
        resp = session.get(url, params=params, headers=HEADERS, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        logger.warning(f"Détail {eightfold_id} — erreur : {e}")
        return None

    # Positions array dans la réponse détail
    positions = data.get("positions", [])
    pos = positions[0] if positions else data  # parfois réponse directe

    # custom_data peut être dict ou str JSON
    raw_cd = pos.get("custom_data") or {}
    if isinstance(raw_cd, str):
        try:
            raw_cd = json.loads(raw_cd)
        except Exception:
            raw_cd = {}

    job_type  = raw_cd.get("jobType") or raw_cd.get("job_type") or ""
    work_style = raw_cd.get("workStyle") or raw_cd.get("work_style") or pos.get("work_location_option") or ""
    brand      = raw_cd.get("brand") or "HSBC"

    posted_date_raw = (
        raw_cd.get("postingStartDate") or
        raw_cd.get("postedDate") or
        raw_cd.get("posted_date") or ""
    )
    # Normalisation date → YYYY-MM-DD
    pub_date = _parse_date(posted_date_raw) or _ts_to_date(pos.get("t_create"))

    desc_html = pos.get("job_description") or ""
    desc_text = _html_to_text(desc_html)

    return {
        "job_description": desc_text,
        "job_type_raw":    job_type,
        "work_style":      work_style,
        "brand":           brand,
        "publication_date": pub_date,
    }


def _parse_date(raw: str) -> str:
    """Tente de parser une date textuelle et retourne YYYY-MM-DD."""
    if not raw:
        return ""
    # "15 May 2026" ou "2026-05-15"
    for fmt in ("%d %B %Y", "%d %b %Y", "%Y-%m-%d", "%m/%d/%Y", "%d/%m/%Y"):
        try:
            return datetime.strptime(raw.strip(), fmt).strftime("%Y-%m-%d")
        except ValueError:
            pass
    return ""


def _ts_to_date(ts) -> str:
    """Convertit un timestamp Unix en YYYY-MM-DD."""
    try:
        return datetime.fromtimestamp(int(ts), tz=timezone.utc).strftime("%Y-%m-%d")
    except Exception:
        return ""


# ═══════════════════════════════════════════════════════════════
# TRANSFORMATION  —  position Eightfold → dict DB
# ═══════════════════════════════════════════════════════════════

def transform_position(pos: Dict, detail: Optional[Dict] = None) -> Dict:
    """
    Convertit un objet position Eightfold (+ détail optionnel) en dict DB.
    """
    eightfold_id    = str(pos.get("id", ""))
    ats_job_id      = str(pos.get("ats_job_id") or pos.get("display_job_id") or "")
    title           = (pos.get("posting_name") or pos.get("name") or "").strip()
    canonical_url   = pos.get("canonicalPositionUrl") or ""
    job_url         = canonical_url or f"https://portal.careers.hsbc.com/careers/job/{eightfold_id}"

    # Localisation
    location_raw   = pos.get("location") or (pos.get("locations") or [""])[0]
    display_loc, country, region = parse_location(location_raw)

    # Date de publication
    pub_date = _ts_to_date(pos.get("t_create"))

    # Métadonnées initiales (à enrichir via détail)
    raw_cd = pos.get("custom_data") or {}
    if isinstance(raw_cd, str):
        try:
            raw_cd = json.loads(raw_cd)
        except Exception:
            raw_cd = {}
    brand = raw_cd.get("brand") or "HSBC"
    work_style = pos.get("work_location_option") or ""

    # Description initiale (souvent vide, enrichie en Phase 2)
    desc_html = pos.get("job_description") or ""
    desc_text = _html_to_text(desc_html)

    # Champs enrichis par le détail
    job_type_raw = ""
    if detail:
        if detail.get("job_description"):
            desc_text = detail["job_description"]
        job_type_raw = detail.get("job_type_raw", "")
        if detail.get("work_style"):
            work_style = detail["work_style"]
        if detail.get("brand"):
            brand = detail["brand"]
        if detail.get("publication_date"):
            pub_date = detail["publication_date"]

    contract_type = parse_contract_type(job_type_raw, title)

    # Famille de métier
    department = (pos.get("department") or pos.get("business_unit") or "").strip()
    job_family = classify_job_family(title, desc_text) or _map_department(department)

    # NLP expérience + éducation
    experience_level = extract_experience_level(desc_text, contract_type, title)
    education_level  = extract_education_level(desc_text, contract_type, title)

    # Management (Director → Oui)
    is_management = _is_management(title, department)

    # Nom de la société selon brand
    company_name = _brand_to_company(brand)

    return {
        "job_url":           job_url,
        "job_id":            ats_job_id or eightfold_id,
        "job_title":         title,
        "contract_type":     contract_type,
        "publication_date":  pub_date,
        "location":          display_loc,
        "country":           country,
        "region":            region,
        "job_family":        job_family,
        "work_style":        _normalize_work_style(work_style),
        "management_position": "Oui" if is_management else "Non",
        "status":            "Live",
        "education_level":   education_level or "",
        "experience_level":  experience_level or "",
        "job_description":   desc_text,
        "company_name":      company_name,
        "brand":             brand,
        "eightfold_id":      eightfold_id,
        "ats_job_id":        ats_job_id,
        "is_valid":          1,
    }


# ─── Helpers transformation ──────────────────────────────────────

_DEPARTMENT_MAP = {
    "technology":                   "Informatique et Digital",
    "risk and compliance":          "Risques et Conformité",
    "asset and wealth management":  "Gestion d'actifs",
    "commercial banking":           "Banque Commerciale",
    "corp & inst banking":          "Banque de Financement et d'Investissement",
    "corporate & institutional":    "Banque de Financement et d'Investissement",
    "investment banking":           "Banque de Financement et d'Investissement",
    "retail banking":               "Banque de Détail",
    "private banking":              "Banque Privée",
    "finance":                      "Finance et Comptabilité",
    "human resources":              "Ressources Humaines",
    "legal":                        "Juridique",
    "operations":                   "Opérations",
    "marketing":                    "Marketing et Communication",
    "strategy":                     "Stratégie",
    "audit":                        "Audit et Contrôle Interne",
}

def _map_department(department: str) -> str:
    if not department:
        return ""
    d = department.lower()
    for key, val in _DEPARTMENT_MAP.items():
        if key in d:
            return val
    return department


def _is_management(title: str, department: str) -> bool:
    t = (title or "").lower()
    return bool(re.search(
        r"\b(?:director|directeur|manager|responsable|head|vp|vice.president"
        r"|managing|président|partner|chief)\b", t
    ))


def _brand_to_company(brand: str) -> str:
    brand_map = {
        "hang seng bank":          "Hang Seng Bank",
        "hsbc innovation banking": "HSBC Innovation Banking",
        "hsbc":                    "HSBC",
    }
    return brand_map.get((brand or "").lower(), COMPANY_NAME)


def _normalize_work_style(ws: str) -> str:
    ws_lower = (ws or "").lower()
    if "hybrid" in ws_lower:
        return "Hybride"
    if "home" in ws_lower or "remote" in ws_lower:
        return "Télétravail"
    if "office" in ws_lower or "onsite" in ws_lower or "on-site" in ws_lower:
        return "Présentiel"
    return ws


# ═══════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════

def main():
    db = Database(DB_PATH)
    session = requests.Session()
    t0 = time.time()

    # ── Phase 1 : collecte liste complète ─────────────────────
    logger.info("=" * 60)
    logger.info("Phase 1 — Collecte de la liste via API Eightfold")
    logger.info("=" * 60)

    raw_positions = fetch_all_jobs(session)
    if not raw_positions:
        logger.error("Phase 1 échouée — aucune offre collectée. Arrêt.")
        return

    api_eightfold_ids = {str(p["id"]) for p in raw_positions}
    logger.info(f"Phase 1 — {len(raw_positions)} offres collectées")

    # Marquer Expired les offres absentes de l'API
    live_ids_db = db.get_live_eightfold_ids()
    newly_expired_ids = live_ids_db - api_eightfold_ids
    if newly_expired_ids:
        # Récupère les URLs correspondantes
        with sqlite3.connect(DB_PATH) as conn:
            ph = ",".join("?" * len(newly_expired_ids))
            expired_urls = {
                r[0] for r in conn.execute(
                    f"SELECT job_url FROM jobs WHERE eightfold_id IN ({ph})",
                    tuple(newly_expired_ids)
                ).fetchall()
            }
        db.mark_expired(expired_urls)
        logger.info(f"  → {len(newly_expired_ids)} offres expirées marquées")

    # Upsert toutes les positions avec infos de base
    new_count = 0
    for pos in raw_positions:
        job_dict = transform_position(pos)
        is_new = db.upsert(job_dict)
        if is_new:
            new_count += 1

    logger.info(f"Phase 1 — {len(raw_positions)} upsertés ({new_count} nouvelles)")

    # ── Phase 2 : enrichissement des descriptions (delta) ─────
    urls_to_enrich = db.get_urls_without_description()
    if urls_to_enrich:
        logger.info("=" * 60)
        logger.info(f"Phase 2 — {len(urls_to_enrich)} offres à enrichir")
        logger.info("=" * 60)

        # Map job_url → eightfold_id pour les URLs à enrichir
        with sqlite3.connect(DB_PATH) as conn:
            rows = conn.execute(
                "SELECT job_url, eightfold_id, job_title, contract_type FROM jobs "
                "WHERE job_url IN ({})".format(",".join("?" * len(urls_to_enrich))),
                tuple(urls_to_enrich),
            ).fetchall()

        done = 0
        for job_url, eid, title, ct in rows:
            if not eid:
                continue
            detail = fetch_detail(session, eid)
            if detail:
                desc_text = detail.get("job_description", "")
                job_type  = detail.get("job_type_raw", "")
                # Re-parse contrat avec info détail
                new_ct = parse_contract_type(job_type, title)

                edu = extract_education_level(desc_text, new_ct, title)
                exp = extract_experience_level(desc_text, new_ct, title)

                with sqlite3.connect(DB_PATH) as conn:
                    conn.execute("""
                        UPDATE jobs SET
                            job_description  = COALESCE(NULLIF(?,  ''), job_description),
                            contract_type    = CASE WHEN ? != '' THEN ? ELSE contract_type END,
                            work_style       = CASE WHEN ? != '' THEN ? ELSE work_style END,
                            brand            = CASE WHEN ? != '' THEN ? ELSE brand END,
                            publication_date = CASE WHEN ? != '' THEN ? ELSE publication_date END,
                            education_level  = COALESCE(NULLIF(?,  ''), education_level),
                            experience_level = COALESCE(NULLIF(?,  ''), experience_level),
                            last_updated     = CURRENT_TIMESTAMP
                        WHERE job_url = ?
                    """, (
                        desc_text,
                        new_ct, new_ct,
                        _normalize_work_style(detail.get("work_style", "")),
                        _normalize_work_style(detail.get("work_style", "")),
                        detail.get("brand", ""),
                        detail.get("brand", ""),
                        detail.get("publication_date", ""),
                        detail.get("publication_date", ""),
                        edu, exp,
                        job_url,
                    ))
                    conn.commit()

            done += 1
            if done % 50 == 0:
                logger.info(f"  Phase 2 : {done}/{len(rows)} offres enrichies")
            time.sleep(DETAIL_DELAY)

        logger.info(f"Phase 2 terminée — {done} offres enrichies")
    else:
        logger.info("Phase 2 — aucune nouvelle offre à enrichir")

    # ── Phase 3 : backfill éducation / expérience ─────────────
    updated = db.backfill_education_experience()
    if updated:
        logger.info(f"Phase 3 — {updated} éducation/expérience inférés")

    # ── Stats finales ─────────────────────────────────────────
    total_live = db.count_live()
    elapsed    = time.time() - t0
    logger.info("=" * 60)
    logger.info(f"✅ HSBC — {total_live} offres Live en base")
    logger.info(f"   Durée  : {elapsed:.0f}s")
    logger.info(f"   Base   : {DB_PATH}")
    logger.info("=" * 60)

    # Distribution par pays
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
