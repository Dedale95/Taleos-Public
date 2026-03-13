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
from datetime import datetime
from pathlib import Path
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

def run_script(script_name, cwd=PYTHON_DIR, timeout=3600):
    print(f"🚀 Lancement de {script_name}...")
    try:
        result = subprocess.run([sys.executable, script_name], 
                              cwd=cwd, capture_output=True, text=True, timeout=timeout)
        if result.returncode == 0:
            print(f"✅ {script_name} terminé avec succès")
            return True
        else:
            print(f"⚠️ {script_name} a échoué (code {result.returncode})")
            print(f"Erreur: {result.stderr[:500]}")
            return False
    except Exception as e:
        print(f"❌ Erreur lors de l'exécution de {script_name}: {e}")
        return False

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
        parts = loc.split(' - ', 1)
        city = (parts[0] or '').strip()
        country = (parts[1] or '').strip()
        if not city or country.lower() != 'france':
            return loc
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
            return []
        
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

    # 1. Scraper Crédit Agricole
    run_script("credit_agricole_scraper.py")
    
    # 2. Scraper Société Générale
    run_script("societe_generale_scraper_improved.py")

    # 3. Scraper Deloitte
    run_script("deloitte_scraper.py")

    # 4. Scraper BNP Paribas
    run_script("bnp_paribas_scraper.py")

    # 5. Scraper BPCE
    run_script("bpce_scraper.py")

    # 6. Scraper Bpifrance
    run_script("bpifrance_scraper.py")

    # 6. Fusion des données depuis les bases SQLite
    merge_from_databases()

    # 7. Export JSON pour les fichiers HTML
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

    # 8. Récapitulatif visuel Live vs Expired par entité
    print()
    print("=" * 60)
    print("📊 OFFRES PAR ENTITÉ (Live / Expired)")
    print("=" * 60)
    try:
        total_live = 0
        total_expired = 0
        print(f"   {'Entité':<22} │ {'Live':>6} │ {'Expired':>7}")
        print("   " + "-" * 40)
        for name, db_path in [("Crédit Agricole", CA_DB), ("Société Générale", SG_DB), ("Deloitte", DELOITTE_DB), ("BNP Paribas", BNP_DB), ("BPCE", BPCE_DB), ("Bpifrance", BPIFRANCE_DB)]:
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

