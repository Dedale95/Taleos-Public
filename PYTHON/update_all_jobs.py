#!/usr/bin/env python3
"""
Script principal pour mettre à jour toutes les offres d'emploi
- Scrape Crédit Agricole
- Scrape Société Générale
- Fusionne dans scraped_jobs.csv
"""

import subprocess
import sys
import csv
import os
import re
import sqlite3
import json
import requests
import time
from datetime import datetime
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional

from country_normalizer import get_country_from_city, normalize_country

# Configuration des chemins
BASE_DIR = Path(__file__).parent.parent
PYTHON_DIR = BASE_DIR / "PYTHON"
HTML_DIR = BASE_DIR / "HTML"
OUTPUT_CSV = HTML_DIR / "scraped_jobs.csv"

# Chemins des bases de données SQLite - source principale maintenant
CA_DB = PYTHON_DIR / "credit_agricole_jobs.db"
SG_DB = PYTHON_DIR / "societe_generale_jobs.db"
DELOITTE_DB = PYTHON_DIR / "deloitte_jobs.db"
BNP_DB = PYTHON_DIR / "bnp_paribas_jobs.db"
BPIFRANCE_DB = PYTHON_DIR / "bpifrance_jobs.db"
BPCE_DB = PYTHON_DIR / "bpce_jobs.db"
CREDIT_MUTUEL_DB = PYTHON_DIR / "credit_mutuel_jobs.db"
ODDO_BHF_DB = PYTHON_DIR / "oddo_bhf_jobs.db"
JP_MORGAN_DB = PYTHON_DIR / "jp_morgan_jobs.db"
GOLDMAN_SACHS_DB = PYTHON_DIR / "goldman_sachs_jobs.db"

EXPIRED_PAGE_PATTERNS = [
    "la page que vous recherchez est introuvable",
    "page introuvable",
    "offre non disponible",
    "offre n'est plus en ligne",
    "offre expirée",
    "error 404",
    "page not found",
    "job position is no longer online",
    "the requested page no longer exists",
]

INCONCLUSIVE_STATUS_CODES = {401, 403, 429}

SHELL_PAGE_PATTERNS = {
    "BPCE": [
        "le groupe bpce, 2e groupe bancaire en france",
        "nos offres d'emploi",
        "rejoindre le groupe bpce",
    ],
}


def _normalize_offer_url_for_compare(url: str) -> str:
    raw = (url or "").strip()
    if not raw:
        return ""
    return raw.rstrip("/")


def _get_current_live_urls_for_source(source_name: str) -> Optional[set[str]]:
    """Charge la liste d'URLs actuellement exposées par la source quand un endpoint fiable existe."""
    if source_name != "BPCE":
        return None

    try:
        from bpce_scraper import fetch_all_jobs_from_api, transform_api_item_to_job

        current_urls = set()
        for item in fetch_all_jobs_from_api():
            job = transform_api_item_to_job(item)
            normalized = _normalize_offer_url_for_compare(job.get("job_url", ""))
            if normalized:
                current_urls.add(normalized)
        print(f"   ↳ {source_name}: {len(current_urls)} URL live chargées depuis l'API source")
        return current_urls
    except Exception as exc:
        print(f"⚠️ {source_name}: impossible de charger les URL live source ({exc})")
        return None


def _db_has_jobs_table(db_path: Path) -> bool:
    if not db_path.exists():
        return False
    conn = sqlite3.connect(db_path)
    try:
        row = conn.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'jobs'"
        ).fetchone()
        return bool(row)
    finally:
        conn.close()


def _is_offer_url_expired(
    url: str,
    source_name: str = "",
    current_live_urls: Optional[set[str]] = None,
    timeout_sec: int = 12,
) -> bool:
    """Détecte rapidement si une URL d'offre est expirée/introuvable."""
    raw = (url or "").strip()
    if not raw:
        return True
    normalized_raw = _normalize_offer_url_for_compare(raw)
    low = normalized_raw.lower()
    if "/404/" in low or low.endswith("/404"):
        return True
    if current_live_urls is not None and normalized_raw not in current_live_urls:
        return True
    try:
        resp = requests.get(
            raw,
            timeout=timeout_sec,
            allow_redirects=True,
            headers={"User-Agent": "Mozilla/5.0 (Taleos-Revalidator)"},
        )
        final_url = (resp.url or raw).lower()
        if resp.status_code in INCONCLUSIVE_STATUS_CODES:
            return False
        if resp.status_code in {404, 410}:
            return True
        if resp.status_code >= 500:
            return False
        if resp.status_code >= 400:
            return True
        if "/404/" in final_url or final_url.endswith("/404"):
            return True
        txt = (resp.text or "")[:20000].lower()
        if any(p in txt for p in EXPIRED_PAGE_PATTERNS):
            return True
        for pattern in SHELL_PAGE_PATTERNS.get(source_name, []):
            if pattern in txt:
                return True
        return False
    except Exception:
        # En cas d'erreur réseau, on ne force pas l'expiration
        return False


def revalidate_live_offers_in_db(
    db_path: Path,
    source_name: str,
    max_workers: int = 20,
    max_urls: Optional[int] = None,
):
    """Revalide toutes les offres Live d'une base et expire celles devenues introuvables."""
    if not db_path.exists():
        print(f"⚠️ Revalidation ignorée ({source_name}) : base absente")
        return 0
    if not _db_has_jobs_table(db_path):
        print(f"⚠️ Revalidation ignorée ({source_name}) : table jobs absente")
        return 0

    conn = sqlite3.connect(db_path)
    try:
        rows = conn.execute(
            "SELECT job_url FROM jobs WHERE status = 'Live' AND is_valid = 1"
        ).fetchall()
        urls = [r[0] for r in rows if r and r[0]]
        if max_urls is not None and len(urls) > max_urls:
            print(
                f"   ↳ {source_name}: plafond revalidation {max_urls}/{len(urls)} URL "
                f"(TALEOS_REVALIDATE_MAX_PER_SOURCE)"
            )
            urls = urls[:max_urls]
        if not urls:
            print(f"🔎 Revalidation {source_name}: 0 URL Live à vérifier")
            return 0

        print(f"🔎 Revalidation {source_name}: vérification de {len(urls)} URL Live...")
        to_expire = set()
        current_live_urls = _get_current_live_urls_for_source(source_name)

        with ThreadPoolExecutor(max_workers=max_workers) as ex:
            futures = {
                ex.submit(_is_offer_url_expired, u, source_name, current_live_urls): u
                for u in urls
            }
            for fut in as_completed(futures):
                u = futures[fut]
                try:
                    if fut.result():
                        to_expire.add(u)
                except Exception:
                    continue

        if to_expire:
            conn.executemany(
                "UPDATE jobs SET status = 'Expired', last_updated = CURRENT_TIMESTAMP WHERE job_url = ?",
                [(u,) for u in to_expire],
            )
            conn.commit()
        print(f"   ↳ {source_name}: {len(to_expire)} offres revalidées comme expirées")
        return len(to_expire)
    finally:
        conn.close()


def revalidate_live_offers_all_sources():
    """Passe globale de revalidation sur toutes les bases d'offres."""
    print("\n🔁 Revalidation globale des offres Live (HTTP + détection 404)...")
    raw = (os.environ.get("TALEOS_REVALIDATE_MAX_PER_SOURCE") or "").strip()
    max_per = int(raw) if raw.isdigit() else None
    if max_per is not None:
        print(f"   (max {max_per} URL par source — variable TALEOS_REVALIDATE_MAX_PER_SOURCE)")
    total = 0
    for name, db_path in [
        ("Crédit Agricole", CA_DB),
        ("Société Générale", SG_DB),
        ("Deloitte", DELOITTE_DB),
        ("BNP Paribas", BNP_DB),
        ("BPCE", BPCE_DB),
        ("Bpifrance", BPIFRANCE_DB),
        ("Crédit Mutuel", CREDIT_MUTUEL_DB),
        ("ODDO BHF", ODDO_BHF_DB),
        ("JP Morgan Chase", JP_MORGAN_DB),
        ("Goldman Sachs", GOLDMAN_SACHS_DB),
    ]:
        total += revalidate_live_offers_in_db(
            db_path, name, max_urls=max_per
        )
    print(f"✅ Revalidation globale terminée: {total} offres supplémentaires expirées")
    return total

def _scraper_timeout_sec(script_name: str) -> int:
    """Timeout subprocess par scraper (secondes). BNP est souvent le plus long (~4000+ offres)."""
    default_other = 3600
    default_bnp = 10800  # 3 h — le scraper BNP a déjà dépassé 1 h en CI
    if script_name == "bnp_paribas_scraper.py":
        raw = os.environ.get("TALEOS_BNP_SCRAPER_TIMEOUT_SEC", str(default_bnp)).strip()
    else:
        raw = os.environ.get("TALEOS_SCRAPER_TIMEOUT_SEC", str(default_other)).strip()
    try:
        sec = int(raw)
        return max(120, min(sec, 18_000))  # plafond ~5 h pour rester sous le job GH (6 h)
    except ValueError:
        return default_bnp if script_name == "bnp_paribas_scraper.py" else default_other


def run_script(script_name, cwd=PYTHON_DIR, timeout=None):
    if timeout is None:
        timeout = _scraper_timeout_sec(script_name)
    print(f"🚀 Lancement de {script_name} (timeout {timeout}s)...")
    try:
        start = time.time()
        proc = subprocess.Popen([sys.executable, script_name], cwd=cwd)
        heartbeat_every_sec = 60
        last_heartbeat = 0

        while True:
            rc = proc.poll()
            elapsed = int(time.time() - start)

            if rc is not None:
                break

            # Évite les jobs "silencieux" en CI (GitHub peut tuer un step sans logs).
            if elapsed - last_heartbeat >= heartbeat_every_sec:
                print(f"   ⏳ {script_name} en cours... {elapsed}s écoulées")
                last_heartbeat = elapsed

            if elapsed > timeout:
                print(f"⏱️ Timeout atteint pour {script_name} après {elapsed}s — terminaison du process...")
                proc.terminate()
                try:
                    proc.wait(timeout=20)
                except subprocess.TimeoutExpired:
                    print(f"🛑 {script_name} ne répond pas à SIGTERM, kill forcé.")
                    proc.kill()
                    proc.wait(timeout=10)
                return False

            time.sleep(2)

        if rc == 0:
            print(f"✅ {script_name} terminé avec succès")
            return True
        else:
            print(f"⚠️ {script_name} a échoué (code {rc})")
            return False

    except Exception as e:
        print(f"❌ Erreur lors de l'exécution de {script_name}: {e}")
        return False

def _ensure_db_exists_or_fail(db_path: Path, label: str):
    if db_path.exists():
        return
    raise RuntimeError(
        f"Base {label} manquante ({db_path}). "
        "Arrêt pour éviter un export partiel avec données obsolètes."
    )

def merge_from_databases():
    """Fusionne les données depuis les bases SQLite (dont job_description = texte complet pour recherche par mots-clés)."""
    print(f"🔄 Fusion des données depuis les bases SQLite vers {OUTPUT_CSV}...")
    all_jobs = []
    headers = None

    def fix_location(loc):
        """Corrige les locations incorrectes (ex: Tunis - France → Tunis - Tunisie, N/A - Luxembourg → Luxembourg)"""
        if not loc:
            return loc
        loc_upper = loc.strip().upper()
        if loc_upper.startswith('N/A') and (' - ' in loc or '-' in loc):
            parts = re.split(r'\s*-\s*', loc.strip(), 1)
            if len(parts) >= 2 and parts[0].strip().upper() == 'N/A':
                return parts[1].strip()
        if ' - ' not in loc:
            return loc
        parts = [part.strip() for part in loc.split(' - ') if part and part.strip()]
        if len(parts) < 2:
            return loc
        city = ' - '.join(parts[:-1]).strip()
        country = parts[-1].strip()
        if not city or country.lower() != 'france':
            return loc
        # Aligné front : "Ile-De France" / "Ile-de France" → libellé canonique (filtre Île-de-France)
        if re.match(r'^î?le(\s|-)+de(\s|-)+france$', city, re.IGNORECASE):
            return f"Île-de-France - {country}"
        correct_country = get_country_from_city(city)
        if correct_country:
            return f"{city} - {normalize_country(correct_country)}"
        return loc

    def clean_description(desc):
        """Nettoie les descriptions en remplaçant les retours à la ligne par des espaces"""
        if not desc:
            return desc
        # Remplacer tous les types de retours à la ligne par des espaces
        cleaned = desc.replace('\r\n', ' ').replace('\n', ' ').replace('\r', ' ')
        # Remplacer les espaces multiples par un seul espace
        cleaned = re.sub(r'\s+', ' ', cleaned)
        return cleaned.strip()
    
    def normalize_education_level(edu):
        """Normalise les niveaux d'étude selon les règles :
        - Bac+3 et Bachelor → même niveau
        - Certificat Fédéral de Capacité et Bac → fusionner
        - Inférieur à Bac et Bac → fusionner
        - Master et Bac+5 → fusionner
        """
        if not edu:
            return edu
        
        edu_lower = edu.lower().strip()
        
        # Mapping de normalisation
        education_mapping = {
            # Bachelor → Bac + 3 / L3
            'bachelor': 'Bac + 3 / L3',
            'bac + 3': 'Bac + 3 / L3',
            'bac+3': 'Bac + 3 / L3',
            'licence': 'Bac + 3 / L3',
            'l3': 'Bac + 3 / L3',
            
            # Master → Bac + 5 / M2 et plus
            'master': 'Bac + 5 / M2 et plus',
            'm2': 'Bac + 5 / M2 et plus',
            'mba': 'Bac + 5 / M2 et plus',
            'bac + 5': 'Bac + 5 / M2 et plus',
            'bac+5': 'Bac + 5 / M2 et plus',
            'grande école': 'Bac + 5 / M2 et plus',
            'école d\'ingénieur': 'Bac + 5 / M2 et plus',
            'école de commerce': 'Bac + 5 / M2 et plus',
            
            # Certificat Fédéral de Capacité → Bac
            'certificat fédéral de capacité': 'Bac',
            'cfc': 'Bac',
            'certificat  fédéral de capacité': 'Bac',
            
            # Inférieur à Bac → Bac
            'inférieur à bac': 'Bac',
            'inférieur au bac': 'Bac',
            'sans bac': 'Bac',
            
            # Bac → Bac
            'bac': 'Bac',
            'baccalauréat': 'Bac',
        }
        
        # Vérifier les correspondances exactes d'abord
        for key, value in education_mapping.items():
            if key in edu_lower:
                return value
        
        # Si déjà dans un format standard, le garder
        standard_levels = [
            'Bac', 'Bac + 2 / L2', 'Bac + 3 / L3', 'Bac + 4 / M1',
            'Bac + 5 / M2 et plus'
        ]
        if edu in standard_levels:
            return edu
        
        # Sinon retourner tel quel
        return edu

    def read_from_db(db_path, company_name):
        """Lit les offres depuis une base SQLite"""
        if not db_path.exists():
            print(f"⚠️ Base de données manquante : {db_path}")
            return [], None
        
        try:
            conn = sqlite3.connect(db_path)
            cursor = conn.execute("""
                SELECT 
                    job_id, job_title, contract_type, publication_date, location,
                    job_family, duration, management_position, status,
                    education_level, experience_level, training_specialization,
                    technical_skills, behavioral_skills, tools, languages,
                    job_description, company_name, company_description, job_url,
                    first_seen, last_updated
                FROM jobs 
                WHERE is_valid = 1 AND status = 'Live'
            """)
            
            # Récupérer les noms de colonnes
            column_names = [description[0] for description in cursor.description]
            
            jobs = []
            for row in cursor.fetchall():
                job = dict(zip(column_names, row))
                
                # Fallback publication_date si vide (ex: Deloitte ne fournit pas la date)
                if not job.get('publication_date') or not str(job.get('publication_date', '')).strip():
                    first_seen = job.get('first_seen')
                    if first_seen:
                        job['publication_date'] = str(first_seen)[:10]
                
                # Convertir les JSON strings en listes pour technical_skills et behavioral_skills
                for col in ['technical_skills', 'behavioral_skills']:
                    if job.get(col) and isinstance(job[col], str):
                        try:
                            if job[col].startswith('['):
                                job[col] = ', '.join(json.loads(job[col]))
                            elif job[col].startswith("['"):
                                # Gérer le cas où c'est une string Python au lieu de JSON
                                job[col] = ', '.join(eval(job[col]))
                        except:
                            pass  # Garder la valeur originale si le parsing échoue
                
                # Nettoyer la description
                if 'job_description' in job and job['job_description']:
                    job['job_description'] = clean_description(job['job_description'])
                
                # Normaliser le niveau d'étude
                if 'education_level' in job and job['education_level']:
                    job['education_level'] = normalize_education_level(job['education_level'])
                
                # Corriger les locations incorrectes (ex: Tunis - France → Tunis - Tunisie)
                if 'location' in job and job['location']:
                    job['location'] = fix_location(job['location'])
                
                jobs.append(job)
            
            conn.close()
            return jobs, column_names
        except Exception as e:
            print(f"   ❌ Erreur lors de la lecture de {db_path}: {e}")
            return [], None

    sources_info = [
        ("Crédit Agricole", CA_DB),
        ("Société Générale", SG_DB),
        ("Deloitte", DELOITTE_DB),
        ("BNP Paribas", BNP_DB),
        ("BPCE", BPCE_DB),
        ("Bpifrance", BPIFRANCE_DB),
        ("Crédit Mutuel", CREDIT_MUTUEL_DB),
        ("ODDO BHF", ODDO_BHF_DB),
        ("JP Morgan Chase", JP_MORGAN_DB),
        ("Goldman Sachs", GOLDMAN_SACHS_DB),
    ]

    for name, db_path in sources_info:
        print(f"📁 Lecture de {name} depuis {db_path.name}...")
        jobs, columns = read_from_db(db_path, name)
        
        if jobs:
            if not headers:
                headers = columns
            all_jobs.extend(jobs)
            print(f"   ✅ {len(jobs)} offres lues")
        else:
            print(f"   ⚠️ Aucune offre trouvée dans {db_path.name}")

    if all_jobs:
        # Trier par date de mise à jour décroissante
        # Gérer les dates manquantes en utilisant une chaîne vide
        all_jobs.sort(key=lambda x: x.get('last_updated', '') or '', reverse=True)
        
        with open(OUTPUT_CSV, 'w', encoding='utf-8', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=headers)
            writer.writeheader()
            writer.writerows(all_jobs)
        print(f"✅ Fusion terminée : {len(all_jobs)} jobs sauvegardés dans {OUTPUT_CSV}")
        
        # Afficher la répartition par entreprise
        companies = {}
        for job in all_jobs:
            company = job.get('company_name', 'Unknown')
            companies[company] = companies.get(company, 0) + 1
        print("\n📊 Répartition par entreprise:")
        for company, count in sorted(companies.items(), key=lambda x: x[1], reverse=True):
            print(f"   - {company}: {count} offres")
    else:
        print("❌ Aucun job à fusionner !")

if __name__ == "__main__":
    print("=" * 80)
    print("🚀 MISE À JOUR DES OFFRES D'EMPLOI")
    print("=" * 80)
    print(f"Date : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()

    failures = []
    require_bnp_db = (os.environ.get("TALEOS_REQUIRE_BNP_DB", "").strip() == "1")

    # 1. Scraper Crédit Agricole
    if not run_script("credit_agricole_scraper.py"):
        failures.append("credit_agricole_scraper.py")
    
    # 2. Scraper Société Générale
    if not run_script("societe_generale_scraper_improved.py"):
        failures.append("societe_generale_scraper_improved.py")

    # 3. Scraper Deloitte
    if not run_script("deloitte_scraper.py"):
        failures.append("deloitte_scraper.py")

    # 4. Scraper BNP Paribas
    if not run_script("bnp_paribas_scraper.py"):
        failures.append("bnp_paribas_scraper.py")
    if require_bnp_db:
        _ensure_db_exists_or_fail(BNP_DB, "BNP Paribas")

    # 5. Scraper BPCE
    if not run_script("bpce_scraper.py"):
        failures.append("bpce_scraper.py")

    # 6. Scraper Bpifrance
    if not run_script("bpifrance_scraper.py"):
        failures.append("bpifrance_scraper.py")

    # 7. Scraper Crédit Mutuel
    if not run_script("credit_mutuel_scraper.py"):
        failures.append("credit_mutuel_scraper.py")

    # 7b. Scraper ODDO BHF
    if not run_script("oddo_bhf_scraper.py"):
        failures.append("oddo_bhf_scraper.py")

    # 7c. Scraper JP Morgan Chase
    if not run_script("jp_morgan_scraper.py"):
        failures.append("jp_morgan_scraper.py")

    # 7d. Scraper Goldman Sachs
    if not run_script("goldman_sachs_scraper.py"):
        failures.append("goldman_sachs_scraper.py")

    if failures:
        print("\n❌ Scrapers en échec:")
        for s in failures:
            print(f"   - {s}")
        if require_bnp_db:
            raise RuntimeError("Au moins un scraper a échoué en mode strict (TALEOS_REQUIRE_BNP_DB=1).")

    # 8. Revalidation globale des offres encore marquées Live
    revalidate_live_offers_all_sources()

    # 9. Fusion des données depuis les bases SQLite
    merge_from_databases()

    # 10. Export JSON pour les fichiers HTML
    print()
    print("🔄 Export JSON pour les fichiers HTML...")
    try:
        result = subprocess.run([sys.executable, "export_sqlite_to_json.py"], 
                              cwd=PYTHON_DIR, capture_output=True, text=True, timeout=60)
        if result.returncode == 0:
            print("✅ Export JSON terminé avec succès")
        else:
            print(f"⚠️ Export JSON a échoué (code {result.returncode})")
            print(f"Erreur: {result.stderr[:500]}")
    except Exception as e:
        print(f"⚠️ Erreur lors de l'export JSON: {e}")

    # 11. Récapitulatif visuel Live vs Expired par entité
    print()
    print("=" * 60)
    print("📊 OFFRES PAR ENTITÉ (Live / Expired)")
    print("=" * 60)
    try:
        total_live = 0
        total_expired = 0
        print(f"   {'Entité':<22} │ {'Live':>6} │ {'Expired':>7}")
        print("   " + "-" * 40)
        for name, db_path in [("Crédit Agricole", CA_DB), ("Société Générale", SG_DB), ("Deloitte", DELOITTE_DB), ("BNP Paribas", BNP_DB), ("BPCE", BPCE_DB), ("Bpifrance", BPIFRANCE_DB), ("Crédit Mutuel", CREDIT_MUTUEL_DB), ("ODDO BHF", ODDO_BHF_DB), ("JP Morgan Chase", JP_MORGAN_DB), ("Goldman Sachs", GOLDMAN_SACHS_DB)]:
            if db_path.exists():
                conn = sqlite3.connect(db_path)
                row = conn.execute("""
                    SELECT 
                        SUM(CASE WHEN status = 'Live' AND is_valid = 1 THEN 1 ELSE 0 END) as live,
                        SUM(CASE WHEN status = 'Expired' THEN 1 ELSE 0 END) as expired
                    FROM jobs
                """).fetchone()
                conn.close()
                live, expired = row[0] or 0, row[1] or 0
                total_live += live
                total_expired += expired
                print(f"   {name:<22} │ {live:>6} │ {expired:>7}")
            else:
                print(f"   {name:<22} │   ---  │   ---  (base manquante)")
        print("   " + "-" * 40)
        print(f"   {'TOTAL (exporté)':<22} │ {total_live:>6} │ {total_expired:>7}")
        print("=" * 60)
    except Exception as e:
        print(f"   ⚠️ Erreur stats: {e}")

    print()
    print("=" * 80)
    print("✅ PROCESSUS TERMINÉ")
    print("=" * 80)
