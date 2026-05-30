#!/usr/bin/env python3
"""
Revolut — Scraper Next.js (revolut.com/careers)
=================================================
Phase 1 : Une seule page Playwright sur /careers/ → __NEXT_DATA__ → 686 positions
           (title, team, locations, ID — descriptions vides à ce stade)
Phase 2 : Playwright concurrent sur chaque page /careers/position/{slug}-{id}/
           → __NEXT_DATA__.pageProps.position.description (HTML complet)

Aucune API JSON publique accessible sans auth (Ashby requiert OAuth).
revolut.com bloque les requêtes HTTP simples (403) → Playwright obligatoire.
"""
from __future__ import annotations

import asyncio
import logging
import re
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Optional

# ───────────────────────────────── Playwright ────────────────────────────────
try:
    from playwright.async_api import async_playwright, BrowserContext, Page
    PLAYWRIGHT_AVAILABLE = True
except ImportError:
    PLAYWRIGHT_AVAILABLE = False

# ───────────────────────────────── Logging ──────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(Path(__file__).parent / "revolut_scraper.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger(__name__)

# ────────────────────────────────── Config ───────────────────────────────────
DB_PATH       = Path(__file__).parent / "revolut_jobs.db"
CAREERS_URL   = "https://www.revolut.com/careers/"
JOB_BASE_URL  = "https://www.revolut.com/careers/position"
CONCURRENCY   = 8        # pages Playwright simultanées (detail)
PAGE_TIMEOUT  = 20_000   # ms
HEADLESS      = True
USER_AGENT    = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)

# ──────────────────────────── Correspondances pays ───────────────────────────
COUNTRY_MAP: dict[str, tuple[str, str]] = {
    "united kingdom":           ("Royaume-Uni",          "Europe"),
    "uk":                       ("Royaume-Uni",          "Europe"),
    "united states":            ("États-Unis",           "Amérique du Nord"),
    "usa":                      ("États-Unis",           "Amérique du Nord"),
    "united states of america": ("États-Unis",           "Amérique du Nord"),
    "canada":                   ("Canada",               "Amérique du Nord"),
    "ireland":                  ("Irlande",              "Europe"),
    "poland":                   ("Pologne",              "Europe"),
    "portugal":                 ("Portugal",             "Europe"),
    "germany":                  ("Allemagne",            "Europe"),
    "france":                   ("France",               "Europe"),
    "netherlands":              ("Pays-Bas",             "Europe"),
    "spain":                    ("Espagne",              "Europe"),
    "italy":                    ("Italie",               "Europe"),
    "switzerland":              ("Suisse",               "Europe"),
    "luxembourg":               ("Luxembourg",           "Europe"),
    "sweden":                   ("Suède",                "Europe"),
    "denmark":                  ("Danemark",             "Europe"),
    "finland":                  ("Finlande",             "Europe"),
    "norway":                   ("Norvège",              "Europe"),
    "czech republic":           ("République tchèque",   "Europe"),
    "romania":                  ("Roumanie",             "Europe"),
    "hungary":                  ("Hongrie",              "Europe"),
    "austria":                  ("Autriche",             "Europe"),
    "greece":                   ("Grèce",                "Europe"),
    "belgium":                  ("Belgique",             "Europe"),
    "croatia":                  ("Croatie",              "Europe"),
    "lithuania":                ("Lituanie",             "Europe"),
    "latvia":                   ("Lettonie",             "Europe"),
    "estonia":                  ("Estonie",              "Europe"),
    "malta":                    ("Malte",                "Europe"),
    "cyprus":                   ("Chypre",               "Europe"),
    "australia":                ("Australie",            "Asie-Pacifique"),
    "new zealand":              ("Nouvelle-Zélande",     "Asie-Pacifique"),
    "india":                    ("Inde",                 "Asie-Pacifique"),
    "singapore":                ("Singapour",            "Asie-Pacifique"),
    "japan":                    ("Japon",                "Asie-Pacifique"),
    "hong kong":                ("Hong Kong",            "Asie-Pacifique"),
    "china":                    ("Chine",                "Asie-Pacifique"),
    "south korea":              ("Corée du Sud",         "Asie-Pacifique"),
    "korea":                    ("Corée du Sud",         "Asie-Pacifique"),
    "indonesia":                ("Indonésie",            "Asie-Pacifique"),
    "malaysia":                 ("Malaisie",             "Asie-Pacifique"),
    "philippines":              ("Philippines",          "Asie-Pacifique"),
    "thailand":                 ("Thaïlande",            "Asie-Pacifique"),
    "vietnam":                  ("Vietnam",              "Asie-Pacifique"),
    "taiwan":                   ("Taïwan",               "Asie-Pacifique"),
    "united arab emirates":     ("Émirats arabes unis",  "Moyen-Orient / Afrique"),
    "uae":                      ("Émirats arabes unis",  "Moyen-Orient / Afrique"),
    "brazil":                   ("Brésil",               "Amérique du Sud"),
    "mexico":                   ("Mexique",              "Amérique du Nord"),
    "colombia":                 ("Colombie",             "Amérique du Sud"),
    "argentina":                ("Argentine",            "Amérique du Sud"),
    "chile":                    ("Chili",                "Amérique du Sud"),
    "nigeria":                  ("Nigeria",              "Moyen-Orient / Afrique"),
    "south africa":             ("Afrique du Sud",       "Moyen-Orient / Afrique"),
    "kenya":                    ("Kenya",                "Moyen-Orient / Afrique"),
    "ghana":                    ("Ghana",                "Moyen-Orient / Afrique"),
}

# Mapping team Revolut → famille de métier Taleos
TEAM_FAMILY_MAP: dict[str, str] = {
    "engineering":              "IT, Digital et Data",
    "data":                     "IT, Digital et Data",
    "product & design":         "IT, Digital et Data",
    "product":                  "IT, Digital et Data",
    "design":                   "IT, Digital et Data",
    "security":                 "Cybersécurité",
    "risk, compliance & audit": "Conformité / Sécurité financière",
    "risk":                     "Risques / Contrôles permanents",
    "compliance":               "Conformité / Sécurité financière",
    "legal":                    "Juridique",
    "finance":                  "Finances / Comptabilité / Contrôle de gestion",
    "treasury":                 "Finances / Comptabilité / Contrôle de gestion",
    "accounting":               "Finances / Comptabilité / Contrôle de gestion",
    "marketing & comms":        "Marketing et Communication",
    "marketing":                "Marketing et Communication",
    "communications":           "Marketing et Communication",
    "operations":               "Gestion des opérations",
    "customer support":         "Gestion des opérations",
    "credit":                   "Risques / Contrôles permanents",
    "people & recruitment":     "Ressources Humaines",
    "people":                   "Ressources Humaines",
    "business development":     "Conseil Clientèle Entreprises",
    "sales":                    "Conseil Clientèle Entreprises",
    "executive":                "Direction générale",
    "strategy":                 "Direction générale",
}

# ─────────────────────── Classification niveau d'expérience ─────────────────
LEVEL_PATTERNS: list[tuple[re.Pattern, str]] = [
    (re.compile(r"\b(head of|cto|cpo|cfo|coo|ciso|chief)\b",  re.I), "11 ans et plus"),
    (re.compile(r"\b(director)\b",                              re.I), "11 ans et plus"),
    (re.compile(r"\b(principal|staff engineer|distinguished)\b",re.I), "11 ans et plus"),
    (re.compile(r"\b(lead|senior lead)\b",                      re.I), "6 - 10 ans"),
    (re.compile(r"\b(senior|sr\.)\b",                           re.I), "3 - 5 ans"),
    (re.compile(r"\b(manager)\b",                               re.I), "3 - 5 ans"),
    (re.compile(r"\b(engineer|analyst|associate|officer|specialist)\b", re.I), "0 - 2 ans"),
    (re.compile(r"\b(intern|internship|graduate|trainee|junior|apprentice)\b", re.I), "0 - 2 ans"),
]


def _classify_level(title: str) -> str:
    for pat, lvl in LEVEL_PATTERNS:
        if pat.search(title):
            return lvl
    return ""


def _normalize_country(raw: str) -> tuple[str, str]:
    key = (raw or "").strip().lower()
    return COUNTRY_MAP.get(key, (raw.title() if raw else "", "Autres"))


def _build_location(locations: list[dict]) -> tuple[str, str, str]:
    """Retourne (location_str, country_fr, region) depuis la liste de localisations Revolut."""
    if not locations:
        return "", "", "Autres"

    # Préférer une localisation de type "office" plutôt que "remote"
    office_locs = [l for l in locations if l.get("type") == "office"]
    primary = office_locs[0] if office_locs else locations[0]

    country_raw = primary.get("country", "")
    country_fr, region = _normalize_country(country_raw)

    # Ville depuis le nom de la localisation (ex: "Tokyo", "London", "Poland - Remote")
    loc_name = primary.get("name", "").split(" - ")[0].strip()
    # Si c'est un pays entier (nom identique au pays), pas de ville
    if loc_name.lower() == country_raw.lower():
        location = country_fr
    elif loc_name and country_fr:
        location = f"{loc_name} - {country_fr}"
    else:
        location = country_fr or loc_name

    # Cas spécial remote only
    if all(l.get("type") == "remote" for l in locations):
        location = f"{country_fr} (Remote)" if country_fr else "Remote"

    return location, country_fr, region


def _title_to_slug(title: str) -> str:
    """Convertit un titre en slug URL Revolut."""
    slug = title.lower()
    slug = re.sub(r"[^a-z0-9\s-]", "", slug)
    slug = re.sub(r"\s+", "-", slug.strip())
    slug = re.sub(r"-+", "-", slug)
    return slug


def _team_to_family(team: str) -> str:
    team_lower = (team or "").lower().strip()
    for key, family in TEAM_FAMILY_MAP.items():
        if key in team_lower:
            return family
    return ""


def _classify_contract(title: str) -> str:
    t = title.lower()
    if any(k in t for k in ["intern", "internship", "stage", "trainee", "apprentice"]):
        return "Stage"
    if any(k in t for k in ["graduate", "junior"]):
        return "CDI"
    return "CDI"


# ──────────────────────────── Database ───────────────────────────────────────
def init_db(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS jobs (
            job_url             TEXT PRIMARY KEY,
            job_id              TEXT,
            job_title           TEXT,
            contract_type       TEXT,
            publication_date    TEXT,
            location            TEXT,
            country             TEXT,
            region              TEXT,
            job_family          TEXT,
            experience_level    TEXT,
            education_level     TEXT,
            job_description     TEXT,
            company_name        TEXT DEFAULT 'Revolut',
            status              TEXT DEFAULT 'Live',
            is_valid            INTEGER DEFAULT 1,
            first_seen          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            last_updated        TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.commit()
    return conn


def upsert_jobs(conn: sqlite3.Connection, jobs: list[dict]) -> int:
    now = datetime.utcnow().isoformat()
    rows = [
        (
            j["job_url"], j["job_id"], j["job_title"], j["contract_type"],
            j["publication_date"], j["location"], j["country"], j["region"],
            j["job_family"], j["experience_level"], j["education_level"],
            j["job_description"], j["company_name"], j["status"], now,
        )
        for j in jobs
    ]
    conn.executemany("""
        INSERT INTO jobs
            (job_url, job_id, job_title, contract_type, publication_date,
             location, country, region, job_family, experience_level,
             education_level, job_description, company_name, status, last_updated)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        ON CONFLICT(job_url) DO UPDATE SET
            job_title        = excluded.job_title,
            contract_type    = excluded.contract_type,
            publication_date = excluded.publication_date,
            location         = excluded.location,
            country          = excluded.country,
            region           = excluded.region,
            job_family       = excluded.job_family,
            experience_level = excluded.experience_level,
            education_level  = excluded.education_level,
            job_description  = excluded.job_description,
            status           = excluded.status,
            last_updated     = excluded.last_updated
    """, rows)
    conn.commit()
    return len(rows)


def get_existing_urls(conn: sqlite3.Connection) -> set[str]:
    return {r[0] for r in conn.execute("SELECT job_url FROM jobs WHERE is_valid=1")}


def mark_expired(conn: sqlite3.Connection, urls: set[str]) -> None:
    if not urls:
        return
    placeholders = ",".join("?" * len(urls))
    conn.execute(
        f"UPDATE jobs SET status='Expired', last_updated=CURRENT_TIMESTAMP "
        f"WHERE job_url IN ({placeholders})",
        list(urls),
    )
    conn.commit()


# ──────────────────────────── Phase 1 : listing ──────────────────────────────
async def fetch_all_positions(context: BrowserContext) -> list[dict]:
    """
    Charge revolut.com/careers/ via Playwright et extrait les 686 positions
    depuis window.__NEXT_DATA__.props.pageProps.positions.
    """
    page = await context.new_page()
    try:
        # "networkidle" timeout car Revolut a des analytics qui keepalive en permanence
        await page.goto(CAREERS_URL, timeout=30_000, wait_until="domcontentloaded")
        await asyncio.sleep(5)  # attendre que Next.js injecte __NEXT_DATA__
        next_data = await page.evaluate("() => window.__NEXT_DATA__")
        if not next_data:
            logger.error("__NEXT_DATA__ absent sur la page Revolut careers")
            return []
        positions = (
            next_data.get("props", {})
            .get("pageProps", {})
            .get("positions", [])
        )
        logger.info(f"Phase 1 terminée — {len(positions)} positions Revolut")
        return positions
    except Exception as exc:
        logger.error(f"Erreur Phase 1 Revolut: {exc}")
        return []
    finally:
        await page.close()


# ──────────────────────────── Phase 2 : détails ──────────────────────────────
async def fetch_detail(context: BrowserContext,
                        sem: asyncio.Semaphore,
                        job_url: str) -> tuple[str, str, str]:
    """
    Retourne (job_url, description_html, pub_date).
    Pubdate: extraite de la description ou date du jour.
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
            await page.goto(job_url, timeout=30_000, wait_until="domcontentloaded")
            await asyncio.sleep(2.5)  # attendre l'hydratation Next.js

            next_data = await page.evaluate("() => window.__NEXT_DATA__")
            if not next_data:
                return job_url, "", ""

            pos = (
                next_data.get("props", {})
                .get("pageProps", {})
                .get("position", {}) or {}
            )
            desc_html = pos.get("description", "") or ""

            # Vérifier expiration
            if not desc_html and not pos.get("text"):
                return job_url, "__EXPIRED__", ""

            # Extraire la date depuis la description (rare) ou laisser vide
            pub_date = datetime.utcnow().strftime("%Y-%m-%d")

            return job_url, desc_html, pub_date

        except Exception as exc:
            logger.warning(f"Detail failed: {job_url} — {exc}")
            return job_url, "", ""
        finally:
            await page.close()


# ────────────────────────────────── Main ─────────────────────────────────────
async def main() -> None:
    if not PLAYWRIGHT_AVAILABLE:
        logger.error("Playwright non installé. Installer : pip install playwright && playwright install chromium")
        return

    logger.info("=== Revolut Scraper (Next.js) ===")
    conn = init_db(DB_PATH)
    existing_urls = get_existing_urls(conn)
    logger.info(f"DB existante : {len(existing_urls)} offres")

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=HEADLESS)
        context = await browser.new_context(user_agent=USER_AGENT)

        # ── Phase 1 : listing ─────────────────────────────────────────────
        positions_raw = await fetch_all_positions(context)
        if not positions_raw:
            logger.error("Aucune position trouvée — abandon")
            await browser.close()
            conn.close()
            return

        # Construire la liste des jobs avec les métadonnées (sans description encore)
        jobs_meta: list[dict] = []
        today = datetime.utcnow().strftime("%Y-%m-%d")
        for pos in positions_raw:
            pos_id    = pos.get("id", "")
            title     = (pos.get("text") or "").strip()
            team      = pos.get("team", "")
            locations = pos.get("locations", [])

            if not pos_id or not title:
                continue

            slug    = _title_to_slug(title)
            job_url = f"{JOB_BASE_URL}/{slug}-{pos_id}/"

            loc_str, country_fr, region = _build_location(locations)

            jobs_meta.append({
                "job_url":          job_url,
                "job_id":           pos_id,
                "job_title":        title,
                "contract_type":    _classify_contract(title),
                "publication_date": today,
                "location":         loc_str,
                "country":          country_fr,
                "region":           region,
                "job_family":       _team_to_family(team),
                "experience_level": _classify_level(title),
                "education_level":  "",
                "job_description":  "",
                "company_name":     "Revolut",
                "status":           "Live",
            })

        api_urls = {j["job_url"] for j in jobs_meta}
        newly_expired = existing_urls - api_urls
        if newly_expired:
            logger.info(f"  → {len(newly_expired)} offres disparues → Expired")
            mark_expired(conn, newly_expired)

        # ── Phase 2 : détails (descriptions) ──────────────────────────────
        logger.info(f"Phase 2 — Fetch descriptions ({len(jobs_meta)} offres, concurrency={CONCURRENCY})…")
        sem = asyncio.Semaphore(CONCURRENCY)
        tasks = [fetch_detail(context, sem, j["job_url"]) for j in jobs_meta]
        url_to_detail: dict[str, tuple[str, str]] = {}
        expired_urls: set[str] = set()
        done = 0

        for coro in asyncio.as_completed(tasks):
            job_url, desc, pub_date = await coro
            if desc == "__EXPIRED__":
                expired_urls.add(job_url)
            else:
                url_to_detail[job_url] = (desc, pub_date)
            done += 1
            if done % 50 == 0:
                logger.info(f"  Phase 2 : {done}/{len(jobs_meta)}")

        await browser.close()

    if expired_urls:
        logger.info(f"  → {len(expired_urls)} offres expirées détectées")
        mark_expired(conn, expired_urls)

    # Enrichir les jobs avec les descriptions
    for job in jobs_meta:
        url = job["job_url"]
        if url in url_to_detail:
            desc_html, pub_date = url_to_detail[url]
            desc_text = re.sub(r"<[^>]+>", " ", desc_html)
            desc_text = re.sub(r"\s+", " ", desc_text).strip()[:25_000]
            job["job_description"] = desc_text
            if pub_date:
                job["publication_date"] = pub_date
        if url in expired_urls:
            job["status"] = "Expired"

    live_jobs = [j for j in jobs_meta if j.get("status") == "Live"]
    saved = upsert_jobs(conn, live_jobs)
    total_live = conn.execute(
        "SELECT COUNT(*) FROM jobs WHERE is_valid=1 AND status='Live'"
    ).fetchone()[0]
    conn.close()
    logger.info(f"✅ Revolut — {saved} offres upsert, {total_live} offres Live en base")


if __name__ == "__main__":
    asyncio.run(main())
