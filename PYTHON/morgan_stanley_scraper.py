#!/usr/bin/env python3
"""
Morgan Stanley — Scraper Eightfold AI (PCSX)
=============================================
Source   : https://morganstanley.eightfold.ai/careers
API list : GET /api/pcsx/search?domain=morganstanley.com&start=N&num=100
API det  : GET /api/pcsx/position_details?position_id=ID&domain=morganstanley.com

Stratégie delta :
  Phase 1 — Listing complet (sans description) : ~15 pages × 100 = ~1 400 offres.
             Coût : rapide, pas de description.
             → Upsert titre/localisation/niveau depuis listing seul.
             → IDs disparus → marqués Expired.

  Phase 2 — Détails delta : uniquement pour les offres sans description en base
             (nouvelles offres + backlog premier run).
             Plafond : DETAIL_CAP par run pour rester dans le timeout CI.

Champs DB standards Taleos :
  job_url, job_id (atsJobId), job_title, contract_type, publication_date,
  location, country, region, job_family, experience_level, education_level,
  job_description, company_name, status, is_valid,
  eightfold_id (PK technique), first_seen, last_updated
"""
from __future__ import annotations

import logging
import re
import sqlite3
import time
from datetime import datetime, timezone
from html.parser import HTMLParser
from pathlib import Path
from typing import Optional

import requests

try:
    from country_normalizer import normalize_country, get_country_from_city
    from job_family_classifier import classify_job_family
    from experience_extractor import extract_experience_level
    from education_extractor import extract_education_level
except ImportError:
    import sys
    sys.path.append(str(Path(__file__).parent))
    from country_normalizer import normalize_country, get_country_from_city
    from job_family_classifier import classify_job_family
    from experience_extractor import extract_experience_level
    from education_extractor import extract_education_level

# ─────────────────────────── Logging ────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(Path(__file__).parent / "morgan_stanley_scraper.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger(__name__)

# ─────────────────────────── Config ─────────────────────────────
DB_PATH      = Path(__file__).parent / "morgan_stanley_jobs.db"
BASE_URL     = "https://morganstanley.eightfold.ai"
DOMAIN       = "morganstanley.com"
LIST_URL     = f"{BASE_URL}/api/pcsx/search"
DETAIL_URL   = f"{BASE_URL}/api/pcsx/position_details"
PAGE_SIZE    = 25           # taille page listing (API retourne au max 25 sans session)
DETAIL_CAP   = 400          # max détails enrichis par run (delta)
DELAY        = 0.3          # secondes entre requêtes détail
COMPANY_NAME = "Morgan Stanley"
TIMEOUT      = 30

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9,fr;q=0.8",
    "Referer": f"{BASE_URL}/careers",
    "Origin": BASE_URL,
}

# ── Mapping niveau Eightfold → expérience Taleos ─────────────────
LEVEL_MAP: dict[str, str] = {
    "managing director":   "11 ans et plus",
    "executive director":  "11 ans et plus",
    "director":            "11 ans et plus",
    "vice president":      "6 - 10 ans",
    "associate":           "3 - 5 ans",
    "analyst":             "0 - 2 ans",
    "intern":              "0 - 2 ans",
    "summer analyst":      "0 - 2 ans",
    "summer associate":    "3 - 5 ans",
    "professional":        "",   # générique, laisser l'extracteur décider
    "associate/analyst":   "0 - 2 ans",
}

# ── Patterns titre → contrat ──────────────────────────────────────
_CONTRACT_PATTERNS = [
    (re.compile(r"\binternship\b|\bintern\b|\bsummer\s+analyst\b|\bstagiaire\b|\bstage\b", re.I), "Stage"),
    (re.compile(r"\balternance\b|\balternant\b|\bapprentice\b|\bapprenti\b", re.I),               "Alternance"),
    (re.compile(r"\bv\.?i\.?e\.?\b|\bvolontariat\s+international\b", re.I),                       "V.I.E."),
    (re.compile(r"\bgraduate\s+program(me)?\b|\bfixed.term\b|\bcdd\b", re.I),                     "CDD"),
    (re.compile(r"\bpart.time\b", re.I),                                                           "Temps partiel"),
]


# ═══════════════════════════════════════════════════════════════
# Helpers
# ═══════════════════════════════════════════════════════════════

class _StripHTML(HTMLParser):
    def __init__(self):
        super().__init__()
        self.parts: list[str] = []
    def handle_data(self, data: str):
        self.parts.append(data)

def html_to_text(html: str) -> str:
    p = _StripHTML()
    p.feed(html or "")
    return " ".join(p.parts).strip()


def parse_contract(time_type_list: list[str], title: str) -> str:
    """Détermine le type de contrat depuis efcustomTextTextTimeType + titre."""
    for pat, ctype in _CONTRACT_PATTERNS:
        if pat.search(title or ""):
            return ctype
    tt = " ".join(time_type_list or []).lower()
    if "fixed" in tt:
        return "CDD"
    if "part" in tt:
        return "Temps partiel"
    return "CDI"


def parse_level(level_list: list[str]) -> str:
    """Mappe efcustomTextPcsPostingJobLevel → experience_level Taleos."""
    for raw in (level_list or []):
        key = raw.strip().lower()
        if key in LEVEL_MAP and LEVEL_MAP[key]:
            return LEVEL_MAP[key]
        # Sous-chaîne
        for k, v in LEVEL_MAP.items():
            if k in key and v:
                return v
    return ""


def parse_location(locations: list[str]) -> tuple[str, str, str]:
    """
    Retourne (location_display, country, region) depuis la liste de lieux Eightfold.
    Format : "City, State, Country" ou "City, Country"
    On prend la première localisation et on normalise le pays.
    """
    if not locations:
        return "", "", ""

    loc_str = locations[0]
    parts = [p.strip() for p in loc_str.split(",")]

    country_raw = parts[-1] if parts else ""
    city_raw    = parts[0]  if parts else ""

    country = normalize_country(country_raw) or ""
    if not country:
        country = normalize_country(get_country_from_city(city_raw) or "") or country_raw

    # Région = ville principale
    region = city_raw

    # Affichage : "Londres - Royaume-Uni" ou si plusieurs lieux "Londres, Paris - ..."
    if len(locations) > 1:
        cities = ", ".join(p.split(",")[0].strip() for p in locations[:3])
        if len(locations) > 3:
            cities += f" (+{len(locations)-3})"
        display = f"{cities} - {country}" if country else cities
    else:
        display = f"{city_raw} - {country}" if country else city_raw

    return display, country, region


def ts_to_date(ts: Optional[int]) -> str:
    """Timestamp UNIX → YYYY-MM-DD."""
    if not ts:
        return ""
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d")


# ═══════════════════════════════════════════════════════════════
# Database
# ═══════════════════════════════════════════════════════════════

def init_db(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS jobs (
            job_url          TEXT PRIMARY KEY,
            job_id           TEXT,             -- atsJobId (JR014245)
            eightfold_id     INTEGER UNIQUE,   -- id numérique Eightfold (clé delta)
            job_title        TEXT,
            contract_type    TEXT,
            publication_date TEXT,
            location         TEXT,
            country          TEXT,
            region           TEXT,
            job_family       TEXT,
            experience_level TEXT,
            education_level  TEXT,
            job_description  TEXT,
            company_name     TEXT DEFAULT 'Morgan Stanley',
            status           TEXT DEFAULT 'Live',
            is_valid         INTEGER DEFAULT 1,
            first_seen       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            last_updated     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    # Migrations pour DB existantes
    existing = {r[1] for r in conn.execute("PRAGMA table_info(jobs)").fetchall()}
    for col, defn in [
        ("eightfold_id", "INTEGER"),
        ("is_valid",     "INTEGER DEFAULT 1"),
    ]:
        if col not in existing:
            conn.execute(f"ALTER TABLE jobs ADD COLUMN {col} {defn}")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_ef_id ON jobs(eightfold_id)")
    conn.commit()
    return conn


def get_live_eightfold_ids(conn: sqlite3.Connection) -> dict[int, str]:
    """Retourne {eightfold_id: job_url} pour toutes les offres Live."""
    return {
        r[0]: r[1]
        for r in conn.execute(
            "SELECT eightfold_id, job_url FROM jobs WHERE status='Live' AND eightfold_id IS NOT NULL"
        )
    }


def get_ids_without_description(conn: sqlite3.Connection) -> list[int]:
    """IDs Eightfold des offres Live sans description (à enrichir)."""
    return [
        r[0] for r in conn.execute(
            "SELECT eightfold_id FROM jobs "
            "WHERE status='Live' AND (job_description IS NULL OR job_description='') "
            "AND eightfold_id IS NOT NULL ORDER BY first_seen DESC"
        )
    ]


def upsert_listing(conn: sqlite3.Connection, job: dict) -> bool:
    """
    Upsert depuis le listing (sans description).
    Retourne True si c'est une nouvelle offre.
    """
    now = datetime.utcnow().isoformat()
    existing = conn.execute(
        "SELECT job_url FROM jobs WHERE eightfold_id=?", (job["eightfold_id"],)
    ).fetchone()
    is_new = existing is None

    conn.execute("""
        INSERT INTO jobs
            (job_url, job_id, eightfold_id, job_title, contract_type, publication_date,
             location, country, region, job_family, experience_level,
             company_name, status, is_valid, first_seen, last_updated)
        VALUES
            (:job_url, :job_id, :eightfold_id, :job_title, :contract_type, :publication_date,
             :location, :country, :region, :job_family, :experience_level,
             :company_name, 'Live', 1, :now, :now)
        ON CONFLICT(job_url) DO UPDATE SET
            job_title        = excluded.job_title,
            contract_type    = excluded.contract_type,
            publication_date = excluded.publication_date,
            location         = excluded.location,
            country          = excluded.country,
            region           = excluded.region,
            job_family       = excluded.job_family,
            experience_level = CASE WHEN excluded.experience_level != '' THEN excluded.experience_level ELSE jobs.experience_level END,
            status           = 'Live',
            is_valid         = 1,
            last_updated     = :now
    """, {**job, "now": now})

    return is_new


def mark_expired(conn: sqlite3.Connection, ef_ids: set[int]) -> int:
    if not ef_ids:
        return 0
    ph = ",".join("?" * len(ef_ids))
    conn.execute(
        f"UPDATE jobs SET status='Expired', is_valid=0, last_updated=datetime('now') "
        f"WHERE eightfold_id IN ({ph})",
        list(ef_ids),
    )
    conn.commit()
    return len(ef_ids)


def update_description(conn: sqlite3.Connection, ef_id: int, detail: dict) -> None:
    conn.execute("""
        UPDATE jobs SET
            job_description  = CASE WHEN ? != '' THEN ? ELSE job_description END,
            contract_type    = CASE WHEN ? != '' THEN ? ELSE contract_type END,
            experience_level = CASE WHEN ? != '' THEN ? ELSE experience_level END,
            education_level  = CASE WHEN ? != '' THEN ? ELSE education_level END,
            last_updated     = datetime('now')
        WHERE eightfold_id = ?
    """, (
        detail["job_description"], detail["job_description"],
        detail["contract_type"],    detail["contract_type"],
        detail["experience_level"], detail["experience_level"],
        detail["education_level"],  detail["education_level"],
        ef_id,
    ))


# ═══════════════════════════════════════════════════════════════
# API
# ═══════════════════════════════════════════════════════════════

def make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update(HEADERS)
    return s


def fetch_listing_page(session: requests.Session, start: int) -> dict:
    r = session.get(
        LIST_URL,
        params={"domain": DOMAIN, "start": start, "num": PAGE_SIZE},
        timeout=TIMEOUT,
    )
    r.raise_for_status()
    return r.json()


def fetch_all_listings(session: requests.Session) -> list[dict]:
    """Récupère toutes les offres du listing (sans description)."""
    all_positions = []
    start = 0

    logger.info("Phase 1 — Listing complet (delta-friendly)...")
    while True:
        try:
            data = fetch_listing_page(session, start)
        except Exception as e:
            logger.error(f"  Erreur page start={start}: {e}")
            break

        positions = data.get("data", {}).get("positions", [])
        if not positions:
            break

        all_positions.extend(positions)
        total = data.get("data", {}).get("count", 0)
        logger.info(f"  start={start:5d} → +{len(positions)} offres (total collecté: {len(all_positions)}/{total})")

        if len(all_positions) >= total or len(positions) == 0:
            break
        # Incrémenter par le nombre réellement retourné (API peut limiter à <PAGE_SIZE)
        start += len(positions)
        time.sleep(0.2)

    logger.info(f"  → {len(all_positions)} offres collectées")
    return all_positions


def fetch_detail(session: requests.Session, ef_id: int) -> Optional[dict]:
    """Récupère les détails d'une offre (description, level, timeType)."""
    try:
        r = session.get(
            DETAIL_URL,
            params={"position_id": ef_id, "domain": DOMAIN},
            timeout=TIMEOUT,
        )
        r.raise_for_status()
        d = r.json().get("data", {})

        title       = d.get("name", "")
        level_list  = d.get("efcustomTextPcsPostingJobLevel", []) or []
        time_list   = d.get("efcustomTextTextTimeType", []) or []
        desc_html   = d.get("jobDescription", "")
        desc_text   = html_to_text(desc_html)

        contract = parse_contract(time_list, title)
        level    = parse_level(level_list) or extract_experience_level(desc_text, contract, title)
        edu      = extract_education_level(desc_text, contract, title)

        return {
            "job_description":  desc_text[:8000],
            "contract_type":    contract,
            "experience_level": level,
            "education_level":  edu,
        }
    except Exception as e:
        logger.warning(f"  Détail ef_id={ef_id} : {e}")
        return None


# ═══════════════════════════════════════════════════════════════
# Transform listing → dict DB
# ═══════════════════════════════════════════════════════════════

def transform_listing(pos: dict) -> dict:
    ef_id   = pos.get("id")
    ats_id  = pos.get("atsJobId") or pos.get("displayJobId") or str(ef_id)
    title   = (pos.get("name") or "").strip()
    dept    = (pos.get("department") or "").strip()

    # Localisation — on prend les locations (liste)
    locations = pos.get("locations") or []
    location, country, region = parse_location(locations)

    # Date de publication (postedTs en priorité, sinon creationTs)
    pub_date = ts_to_date(pos.get("postedTs") or pos.get("creationTs"))

    # Contrat depuis le titre (pas de timeType dans le listing)
    contract = parse_contract([], title)

    # Niveau depuis le titre
    level = ""
    title_lower = title.lower()
    for k, v in LEVEL_MAP.items():
        if k in title_lower and v:
            level = v
            break

    # Famille de métier
    family = classify_job_family(title + " " + dept)

    job_url = f"{BASE_URL}/careers/job/{ef_id}"

    return {
        "job_url":          job_url,
        "job_id":           ats_id,
        "eightfold_id":     ef_id,
        "job_title":        title,
        "contract_type":    contract,
        "publication_date": pub_date,
        "location":         location,
        "country":          country,
        "region":           region,
        "job_family":       family,
        "experience_level": level,
        "company_name":     COMPANY_NAME,
    }


# ═══════════════════════════════════════════════════════════════
# Main
# ═══════════════════════════════════════════════════════════════

def main():
    t0 = time.time()
    conn = init_db(DB_PATH)
    session = make_session()

    # ── Phase 1 : listing complet ──────────────────────────────
    raw_positions = fetch_all_listings(session)
    if not raw_positions:
        logger.error("Aucune offre collectée — arrêt.")
        conn.close()
        return

    # IDs actuellement Live en DB
    live_in_db = get_live_eightfold_ids(conn)  # {ef_id: job_url}
    live_ids_api = {int(p["id"]) for p in raw_positions if p.get("id")}

    # Marquer expirées (présentes en DB, absentes de l'API)
    expired_ids = set(live_in_db.keys()) - live_ids_api
    expired_count = mark_expired(conn, expired_ids)
    if expired_count:
        logger.info(f"🗑️  {expired_count} offre(s) expirée(s) (absentes de l'API)")

    # Upsert toutes les positions du listing
    new_count = 0
    for pos in raw_positions:
        job = transform_listing(pos)
        if not job["eightfold_id"]:
            continue
        is_new = upsert_listing(conn, job)
        if is_new:
            new_count += 1
    conn.commit()

    logger.info(f"Phase 1 — {len(raw_positions)} upsertés | {new_count} nouvelles | {expired_count} expirées")

    # ── Phase 2 : enrichissement descriptions (delta) ─────────
    ids_without_desc = get_ids_without_description(conn)
    if ids_without_desc:
        logger.info("=" * 60)
        to_enrich = ids_without_desc[:DETAIL_CAP]
        backlog   = max(0, len(ids_without_desc) - DETAIL_CAP)
        logger.info(f"Phase 2 — {len(ids_without_desc)} sans description → enrichissement de {len(to_enrich)} (plafond {DETAIL_CAP}/run)")
        if backlog:
            logger.warning(f"⚠️  Backlog : {backlog} offres restantes → traitées aux prochains runs")

        enriched = 0
        for ef_id in to_enrich:
            detail = fetch_detail(session, ef_id)
            if detail:
                update_description(conn, ef_id, detail)
                enriched += 1
            time.sleep(DELAY)

        conn.commit()
        logger.info(f"Phase 2 — {enriched}/{len(to_enrich)} offres enrichies")
    else:
        logger.info("Phase 2 — Toutes les offres ont déjà une description ✅")

    total_live = conn.execute("SELECT COUNT(*) FROM jobs WHERE status='Live'").fetchone()[0]
    elapsed = time.time() - t0

    logger.info(f"\n{'='*60}")
    logger.info(f"✅ {COMPANY_NAME} — {total_live} offres Live en base")
    logger.info(f"   Nouvelles  : {new_count}")
    logger.info(f"   Expirées   : {expired_count}")
    logger.info(f"   Durée      : {elapsed:.0f}s")
    logger.info(f"   Base       : {DB_PATH}")
    logger.info(f"{'='*60}")

    conn.close()


if __name__ == "__main__":
    main()
