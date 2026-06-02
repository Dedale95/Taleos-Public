#!/usr/bin/env python3
"""
Revolut — Scraper (revolut.com/careers)
========================================
Revolut utilise un SPA (React/Next.js) derrière Cloudflare Bot Protection.
`__NEXT_DATA__` n'est plus disponible SSR depuis mi-2026.

Stratégie multi-couches (dans l'ordre) :
  1. playwright-stealth pour contourner la détection Cloudflare
  2. Interception des réponses XHR/fetch du SPA (API JSON interne)
  3. Lecture de window.__NEXT_DATA__ si encore présent
  4. Extraction depuis le DOM (liens /careers/position/)
  5. Fallback : lecture des scripts JSON inline

Phase 2 (descriptions) : même approche sur chaque page de poste.
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

# playwright-stealth : contourne la détection Cloudflare / headless
try:
    from playwright_stealth import stealth_async as _stealth_async
    STEALTH_AVAILABLE = True
except ImportError:
    STEALTH_AVAILABLE = False

# Script de stealth minimal si le package n'est pas dispo
_STEALTH_JS = """
(function () {
    const overwrite = (obj, prop, value) => {
        try { Object.defineProperty(obj, prop, { get: () => value, configurable: true }); }
        catch(e) {}
    };
    overwrite(navigator, 'webdriver', undefined);
    overwrite(navigator, 'plugins', [1, 2, 3, 4, 5]);
    overwrite(navigator, 'languages', ['en-US', 'en']);
    if (!window.chrome) window.chrome = { runtime: {} };
    const orig = window.navigator.permissions.query;
    window.navigator.permissions.query = (params) =>
        params.name === 'notifications'
            ? Promise.resolve({ state: Notification.permission })
            : orig(params);
})();
"""

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
CONCURRENCY    = 6        # pages Playwright simultanées (detail)
PAGE_TIMEOUT   = 40_000   # ms
CF_WAIT_S      = 10       # secondes d'attente pour que le challenge CF se résolve
HEADLESS       = True
USER_AGENT     = (
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
    import re as _re
    t = title.lower()
    # Utiliser des mots entiers pour éviter les faux positifs
    # ex: "Internal" contient "intern" → faux positif sans \b
    if _re.search(r'\b(intern|internship|stage|trainee|apprentice|alternance|alternant)\b', t):
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


# ──────────────────────── Helpers stealth & context ──────────────────────────

async def _apply_stealth(page: Page) -> None:
    """Applique les patches anti-détection sur la page."""
    if STEALTH_AVAILABLE:
        await _stealth_async(page)
    else:
        await page.add_init_script(_STEALTH_JS)


async def _new_stealth_context(browser):
    """Crée un contexte navigateur avec headers et viewport réalistes."""
    context = await browser.new_context(
        user_agent=USER_AGENT,
        viewport={"width": 1280, "height": 800},
        locale="en-US",
        timezone_id="Europe/London",
        extra_http_headers={
            "Accept-Language": "en-US,en;q=0.9",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        },
    )
    return context


def _extract_positions_from_data(data) -> list[dict]:
    """Tente d'extraire une liste de positions depuis un objet JSON quelconque."""
    if isinstance(data, list) and len(data) > 3:
        # Vérifier que ça ressemble à des offres (id + text ou title)
        if data and isinstance(data[0], dict) and (
            "text" in data[0] or "title" in data[0] or "name" in data[0]
        ):
            return data
    if isinstance(data, dict):
        for key in ("positions", "jobs", "data", "results", "openings", "items"):
            val = data.get(key)
            if isinstance(val, list) and len(val) > 3:
                if val and isinstance(val[0], dict):
                    return val
        # Imbriqué : props.pageProps.positions
        props = data.get("props", {}) or {}
        page_props = props.get("pageProps", {}) or {}
        positions = page_props.get("positions")
        if isinstance(positions, list) and positions:
            return positions
    return []


# ──────────────────────────── Phase 1 : listing ──────────────────────────────
async def fetch_all_positions(browser) -> list[dict]:
    """
    Charge revolut.com/careers/ et extrait toutes les positions.
    Stratégies (dans l'ordre) :
      1. window.__NEXT_DATA__ (legacy SSR)
      2. Interception des réponses XHR/fetch JSON du SPA
      3. Scripts JSON inline (<script type="application/json">)
      4. Liens DOM vers /careers/position/
    """
    context = await _new_stealth_context(browser)
    page = await context.new_page()
    await _apply_stealth(page)

    # ── Interception réseau ──────────────────────────────────────────────────
    captured: list[dict] = []

    async def on_response(response):
        ct = response.headers.get("content-type", "")
        if "application/json" not in ct:
            return
        url_lower = response.url.lower()
        # Cibler les URLs qui ressemblent à des endpoints d'offres
        if not any(k in url_lower for k in (
            "/api/", "careers", "positions", "jobs", "openings", "posting"
        )):
            return
        try:
            data = await response.json()
        except Exception:
            return
        found = _extract_positions_from_data(data)
        if found:
            logger.info(f"  [XHR] {len(found)} positions depuis {response.url[:80]}")
            captured.extend(found)

    page.on("response", on_response)

    # ── Chargement de la page ────────────────────────────────────────────────
    try:
        await page.goto(CAREERS_URL, timeout=60_000, wait_until="domcontentloaded")
        # Attendre que le challenge Cloudflare se résolve et que le SPA s'initialise
        await asyncio.sleep(CF_WAIT_S)

        # Attendre networkidle (avec tolérance si keepalives analytics bloquent)
        try:
            await page.wait_for_load_state("networkidle", timeout=20_000)
        except Exception:
            pass
        await asyncio.sleep(3)

        # ── Stratégie 1 : __NEXT_DATA__ ──────────────────────────────────────
        try:
            next_data = await page.evaluate("() => window.__NEXT_DATA__")
            if next_data:
                found = _extract_positions_from_data(next_data)
                if found:
                    logger.info(f"  [__NEXT_DATA__] {len(found)} positions")
                    return found
        except Exception:
            pass

        # ── Stratégie 2 : réponses XHR capturées ─────────────────────────────
        if captured:
            # Dédupliquer par id/text
            seen = set()
            unique = []
            for pos in captured:
                key = pos.get("id") or pos.get("text") or str(pos)
                if key not in seen:
                    seen.add(key)
                    unique.append(pos)
            logger.info(f"  [XHR total] {len(unique)} positions uniques")
            return unique

        # ── Stratégie 3 : scripts JSON inline ────────────────────────────────
        try:
            inline = await page.evaluate("""
                () => {
                    for (const s of document.querySelectorAll(
                        'script[type="application/json"], script#__NEXT_DATA__'
                    )) {
                        try {
                            const d = JSON.parse(s.textContent);
                            if (d && d.positions) return d;
                            if (Array.isArray(d) && d.length > 5 && d[0] && (d[0].id || d[0].text))
                                return d;
                        } catch(e) {}
                    }
                    return null;
                }
            """)
            if inline:
                found = _extract_positions_from_data(inline)
                if found:
                    logger.info(f"  [JSON inline] {len(found)} positions")
                    return found
        except Exception:
            pass

        # ── Stratégie 4 : liens DOM /careers/position/ ───────────────────────
        try:
            links = await page.evaluate("""
                () => {
                    const seen = new Set();
                    const jobs = [];
                    for (const a of document.querySelectorAll('a[href*="/careers/position/"]')) {
                        const href = a.getAttribute('href') || '';
                        if (seen.has(href)) continue;
                        seen.add(href);
                        // Récupérer le titre depuis l'élément ou ses enfants
                        const title = (
                            a.querySelector('[class*="title"],[class*="name"],[class*="heading"]')
                            || a
                        ).textContent.trim();
                        // Extraire l'ID depuis le slug (dernier segment numérique ou alphanumérique)
                        const m = href.match(/([a-zA-Z0-9]+)\\/?$/);
                        jobs.push({ href, title, rawId: m ? m[1] : '' });
                    }
                    return jobs;
                }
            """)
            if links and len(links) > 5:
                logger.info(f"  [DOM links] {len(links)} postes trouvés")
                # Convertir en format compatible avec le reste du pipeline
                positions = []
                for lk in links:
                    positions.append({
                        "id":        lk.get("rawId", ""),
                        "text":      lk.get("title", ""),
                        "team":      "",
                        "locations": [],
                        "_href":     lk.get("href", ""),
                    })
                return positions
        except Exception:
            pass

        logger.error("Aucune position trouvée après toutes les stratégies")
        return []

    except Exception as exc:
        logger.error(f"Erreur Phase 1 Revolut: {exc}")
        return []
    finally:
        await context.close()


# ──────────────────────────── Phase 2 : détails ──────────────────────────────
async def fetch_detail(browser,
                        sem: asyncio.Semaphore,
                        job_url: str) -> tuple[str, str, str]:
    """
    Retourne (job_url, description_html, pub_date).
    Stratégies : __NEXT_DATA__ → XHR → DOM.
    """
    async with sem:
        context = await _new_stealth_context(browser)
        page = await context.new_page()
        await _apply_stealth(page)

        # Bloquer médias/fonts pour aller plus vite
        await page.route(
            "**/*",
            lambda route: route.abort()
            if route.request.resource_type in ("image", "media", "font", "stylesheet")
            else route.continue_(),
        )

        # Intercepter les réponses JSON
        captured_desc = []

        async def on_resp(response):
            ct = response.headers.get("content-type", "")
            if "application/json" not in ct:
                return
            url_lower = response.url.lower()
            if not any(k in url_lower for k in ("/api/", "position", "career", "job")):
                return
            try:
                data = await response.json()
                if isinstance(data, dict):
                    desc = (
                        data.get("description")
                        or (data.get("position") or {}).get("description")
                        or (data.get("data") or {}).get("description")
                    )
                    if desc:
                        captured_desc.append(desc)
            except Exception:
                pass

        page.on("response", on_resp)

        try:
            await page.goto(job_url, timeout=PAGE_TIMEOUT, wait_until="domcontentloaded")
            await asyncio.sleep(5)

            try:
                await page.wait_for_load_state("networkidle", timeout=12_000)
            except Exception:
                pass
            await asyncio.sleep(2)

            # Stratégie 1 : __NEXT_DATA__
            try:
                next_data = await page.evaluate("() => window.__NEXT_DATA__")
                if next_data:
                    pos = (
                        next_data.get("props", {})
                        .get("pageProps", {})
                        .get("position", {}) or {}
                    )
                    desc_html = pos.get("description", "") or ""
                    if not desc_html and not pos.get("text"):
                        return job_url, "__EXPIRED__", ""
                    if desc_html:
                        return job_url, desc_html, datetime.utcnow().strftime("%Y-%m-%d")
            except Exception:
                pass

            # Stratégie 2 : XHR capturé
            if captured_desc:
                return job_url, captured_desc[0], datetime.utcnow().strftime("%Y-%m-%d")

            # Stratégie 3 : DOM
            try:
                desc_dom = await page.evaluate("""
                    () => {
                        // Chercher les blocs de description courants
                        const selectors = [
                            '[class*="description"]',
                            '[class*="content"]',
                            '[data-qa="job-description"]',
                            'article',
                            'main .prose',
                            '.job-description',
                        ];
                        for (const sel of selectors) {
                            const el = document.querySelector(sel);
                            if (el && el.innerText && el.innerText.length > 100)
                                return el.innerHTML;
                        }
                        return null;
                    }
                """)
                if desc_dom:
                    return job_url, desc_dom, datetime.utcnow().strftime("%Y-%m-%d")
            except Exception:
                pass

            # Aucune description trouvée — offre peut-être expirée
            page_text = await page.evaluate("() => document.body.innerText") or ""
            if len(page_text.strip()) < 200:
                return job_url, "__EXPIRED__", ""

            return job_url, "", ""

        except Exception as exc:
            logger.warning(f"Detail failed: {job_url} — {exc}")
            return job_url, "", ""
        finally:
            await context.close()


# ────────────────────────────────── Main ─────────────────────────────────────
async def main() -> None:
    if not PLAYWRIGHT_AVAILABLE:
        logger.error("Playwright non installé. Installer : pip install playwright && playwright install chromium")
        return

    logger.info("=== Revolut Scraper (Next.js) ===")
    conn = init_db(DB_PATH)
    existing_urls = get_existing_urls(conn)
    logger.info(f"DB existante : {len(existing_urls)} offres")

    logger.info(f"playwright-stealth disponible: {STEALTH_AVAILABLE}")

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(
            headless=HEADLESS,
            args=[
                "--no-sandbox",
                "--disable-setuid-sandbox",
                "--disable-dev-shm-usage",
                "--disable-blink-features=AutomationControlled",
            ],
        )

        # ── Phase 1 : listing ─────────────────────────────────────────────
        positions_raw = await fetch_all_positions(browser)
        if not positions_raw:
            logger.error("Aucune position trouvée — abandon")
            await browser.close()
            conn.close()
            return

        # Construire la liste des jobs avec les métadonnées (sans description encore)
        jobs_meta: list[dict] = []
        today = datetime.utcnow().strftime("%Y-%m-%d")
        for pos in positions_raw:
            pos_id    = str(pos.get("id", "") or "").strip()
            title     = (pos.get("text") or pos.get("title") or pos.get("name") or "").strip()
            team      = pos.get("team", "") or pos.get("department", "") or ""
            locations = pos.get("locations", []) or pos.get("location", []) or []
            if isinstance(locations, str):
                locations = [{"name": locations, "country": locations}]

            if not title:
                continue

            # URL : fournie directement (stratégie DOM) ou construite depuis id+slug
            raw_href = pos.get("_href", "")
            if raw_href:
                job_url = raw_href if raw_href.startswith("http") else f"https://www.revolut.com{raw_href}"
            elif pos_id:
                slug    = _title_to_slug(title)
                job_url = f"{JOB_BASE_URL}/{slug}-{pos_id}/"
            else:
                continue

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
        tasks = [fetch_detail(browser, sem, j["job_url"]) for j in jobs_meta]
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
