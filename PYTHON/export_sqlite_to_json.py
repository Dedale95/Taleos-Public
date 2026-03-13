#!/usr/bin/env python3
"""
Script pour exporter les données SQLite vers JSON
Utilisé par les fichiers HTML pour charger les données.

IMPORTANT - Distinction Live / Expirées :
- scraped_jobs.json et scraped_jobs_live.json : UNIQUEMENT offres Live (pour le site Taleos)
- scraped_jobs_full.json : Live + Expired (pour mes-candidatures / référence)
Le site affiche le nombre d'offres live, pas live+expirées.

Recherche par mots-clés :
- La colonne job_description contient le TEXTE COMPLET de l'offre (jusqu'à ~25k caractères).
- Ce texte est exporté dans le JSON et utilisé UNIQUEMENT pour la recherche par mots-clés
  dans offres.html et filtres.html (filtre "Recherche par mots-clés").
- Il n'est PAS affiché sur les vignettes / cartes d'offres (titre, lieu, contrat, famille, etc. uniquement).
"""

import re
import sqlite3
import json
from pathlib import Path
from datetime import datetime
from country_normalizer import get_country_from_city, normalize_country

# Configuration des chemins
PYTHON_DIR = Path(__file__).parent
HTML_DIR = PYTHON_DIR.parent / "HTML"
OUTPUT_JSON = HTML_DIR / "scraped_jobs.json"

# Chemins des bases de données SQLite
CA_DB = PYTHON_DIR / "credit_agricole_jobs.db"
SG_DB = PYTHON_DIR / "societe_generale_jobs.db"
DELOITTE_DB = PYTHON_DIR / "deloitte_jobs.db"
BNP_DB = PYTHON_DIR / "bnp_paribas_jobs.db"

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
    parts = loc.split(' - ', 1)
    city = (parts[0] or '').strip()
    country = (parts[1] or '').strip()
    if not city or country.lower() != 'france':
        return loc
    correct_country = get_country_from_city(city)
    if correct_country:
        return f"{city} - {normalize_country(correct_country)}"
    return loc


def read_from_db(db_path, company_name, live_only=True):
    """Lit les offres depuis une base SQLite.
    live_only=True : uniquement les offres Live (pour affichage site).
    live_only=False : Live + Expired (pour mes-candidatures, référence).
    """
    if not db_path.exists():
        print(f"⚠️ Base de données manquante : {db_path}")
        return []
    
    try:
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row  # Permet d'accéder aux colonnes par nom
        status_filter = "AND status = 'Live'" if live_only else ""
        cursor = conn.execute(f"""
            SELECT 
                job_id, job_title, contract_type, publication_date, location,
                job_family, duration, management_position, status,
                education_level, experience_level, training_specialization,
                technical_skills, behavioral_skills, tools, languages,
                job_description, company_name, company_description, job_url,
                first_seen, last_updated
            FROM jobs 
            WHERE is_valid = 1 {status_filter}
            ORDER BY last_updated DESC
        """)
        
        jobs = []
        for row in cursor.fetchall():
            job = dict(row)
            
            # Corriger les locations incorrectes (ex: Tunis - France → Tunis - Tunisie)
            if job.get('location'):
                job['location'] = fix_location(job['location'])
            
            # Convertir les JSON strings en listes pour technical_skills et behavioral_skills
            for col in ['technical_skills', 'behavioral_skills']:
                if job.get(col) and isinstance(job[col], str):
                    try:
                        if job[col].startswith('['):
                            # C'est déjà du JSON
                            parsed = json.loads(job[col])
                            job[col] = ', '.join(parsed) if isinstance(parsed, list) else job[col]
                        elif job[col].startswith("['"):
                            # C'est une string Python, essayer de l'évaluer (attention sécurité)
                            # Mais on va plutôt essayer de parser manuellement
                            job[col] = job[col]  # Garder tel quel pour l'instant
                    except:
                        pass  # Garder la valeur originale si le parsing échoue
            
            jobs.append(job)
        
        conn.close()
        return jobs
    except Exception as e:
        print(f"   ❌ Erreur lors de la lecture de {db_path}: {e}")
        return []

def main():
    print("=" * 80)
    print("🔄 EXPORT DES DONNÉES SQLITE VERS JSON")
    print("=" * 80)
    print(f"Date : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    
    all_jobs = []
    
    sources_info = [
        ("Crédit Agricole", CA_DB),
        ("Société Générale", SG_DB),
        ("Deloitte", DELOITTE_DB),
        ("BNP Paribas", BNP_DB),
    ]
    
    for name, db_path in sources_info:
        print(f"📁 Lecture de {name} depuis {db_path.name}...")
        jobs = read_from_db(db_path, name, live_only=True)
        
        if jobs:
            all_jobs.extend(jobs)
            print(f"   ✅ {len(jobs)} offres Live lues")
        else:
            print(f"   ⚠️ Aucune offre trouvée dans {db_path.name}")
    
    if all_jobs:
        # Sauvegarder en JSON (version complète)
        with open(OUTPUT_JSON, 'w', encoding='utf-8') as f:
            json.dump(all_jobs, f, ensure_ascii=False, indent=2)
        
        print()
        print(f"✅ Export terminé : {len(all_jobs)} offres Live sauvegardées dans {OUTPUT_JSON.name}")
        print(f"   (Les offres expirées sont exclues du site - voir scraped_jobs_full.json pour référence)")
        
        # Créer une version allégée avec seulement les offres Live (pour GitHub Pages)
        live_jobs = [job for job in all_jobs if job.get('status') == 'Live']
        OUTPUT_JSON_LIVE = HTML_DIR / "scraped_jobs_live.json"
        with open(OUTPUT_JSON_LIVE, 'w', encoding='utf-8') as f:
            json.dump(live_jobs, f, ensure_ascii=False, indent=2)
        
        print(f"✅ Version allégée créée : {len(live_jobs)} offres Live dans {OUTPUT_JSON_LIVE.name}")
        
        # Version complète (Live + Expired) pour mes-candidatures / référence
        all_jobs_full = []
        for name, db_path in sources_info:
            if db_path.exists():
                full = read_from_db(db_path, name, live_only=False)
                all_jobs_full.extend(full)
        OUTPUT_JSON_FULL = HTML_DIR / "scraped_jobs_full.json"
        if all_jobs_full:
            with open(OUTPUT_JSON_FULL, 'w', encoding='utf-8') as f:
                json.dump(all_jobs_full, f, ensure_ascii=False, indent=2)
            live_count = sum(1 for j in all_jobs_full if j.get('status') == 'Live')
            print(f"✅ Version complète créée : {len(all_jobs_full)} offres (dont {live_count} Live) dans {OUTPUT_JSON_FULL.name}")
        
        # Afficher la répartition par entreprise
        companies = {}
        for job in all_jobs:
            company = job.get('company_name', 'Unknown')
            companies[company] = companies.get(company, 0) + 1
        
        print("\n📊 Répartition par entreprise:")
        for company, count in sorted(companies.items(), key=lambda x: x[1], reverse=True):
            print(f"   - {company}: {count} offres")
        
        # Afficher la répartition par statut
        statuses = {}
        for job in all_jobs:
            status = job.get('status', 'Unknown')
            statuses[status] = statuses.get(status, 0) + 1
        
        print("\n📊 Répartition par statut:")
        for status, count in sorted(statuses.items(), key=lambda x: x[1], reverse=True):
            print(f"   - {status}: {count} offres")
        
        # Validation : le site doit afficher uniquement les offres Live
        expired_in_export = sum(1 for j in all_jobs if str(j.get('status', '')).lower().strip() != 'live')
        if expired_in_export > 0:
            print(f"\n⚠️ ATTENTION : {expired_in_export} offres expirées dans l'export site (ne devrait pas arriver)")
        else:
            print(f"\n✅ Validation : {len(all_jobs)} offres Live dans le JSON du site (0 expirées)")
    else:
        print("❌ Aucun job à exporter !")
    
    print()
    print("=" * 80)

if __name__ == "__main__":
    main()
