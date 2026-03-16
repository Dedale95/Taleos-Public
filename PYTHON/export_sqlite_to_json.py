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
import unicodedata
from pathlib import Path
from datetime import datetime
from country_normalizer import get_country_from_city, normalize_country
from experience_extractor import extract_experience_level

# Configuration des chemins
PYTHON_DIR = Path(__file__).parent
HTML_DIR = PYTHON_DIR.parent / "HTML"
OUTPUT_JSON = HTML_DIR / "scraped_jobs.json"

# Chemins des bases de données SQLite
CA_DB = PYTHON_DIR / "credit_agricole_jobs.db"
SG_DB = PYTHON_DIR / "societe_generale_jobs.db"
DELOITTE_DB = PYTHON_DIR / "deloitte_jobs.db"
BNP_DB = PYTHON_DIR / "bnp_paribas_jobs.db"
BPIFRANCE_DB = PYTHON_DIR / "bpifrance_jobs.db"
BPCE_DB = PYTHON_DIR / "bpce_jobs.db"
CREDIT_MUTUEL_DB = PYTHON_DIR / "credit_mutuel_jobs.db"
ODDO_BHF_DB = PYTHON_DIR / "oddo_bhf_jobs.db"


def _normalize_text(s: str) -> str:
    """Normalise une string pour les comparaisons (minuscules, sans accents)."""
    if not s:
        return ""
    s = str(s)
    s = unicodedata.normalize("NFKD", s)
    s = "".join(c for c in s if not unicodedata.combining(c))
    return s.lower()


def normalize_job_family(raw_family: str) -> str:
    """
    Normalise les familles de métier hétérogènes (surtout Deloitte / BPCE / Bpifrance)
    vers un jeu de catégories canonique utilisé par le site.
    """
    if not raw_family:
        return raw_family

    norm = _normalize_text(raw_family)

    # Commercial / Relation client (y compris \"expertise commercial et accompagnement réseau\")
    if "relation client" in norm or "expertise commercial" in norm or "commercial" in norm:
        return "Commercial / Relations Clients"

    # IT / Digital / Data
    if "informatiq" in norm or "technologie" in norm or "technologies" in norm or "data" in norm or "digital" in norm:
        return "IT, Digital et Data"

    # Financement / Investissement / Banque de financement
    if "financement" in norm or "banque de financement" in norm:
        return "Financement et Investissement"

    # Ressources humaines
    if "ressources humaines" in norm or "ressourceshumaines" in norm or "rh" in norm:
        return "Ressources Humaines"

    # Finance / Comptabilité
    if "comptabilite" in norm or "comptabilité" in norm or "finance" in norm:
        return "Finances / Comptabilité / Contrôle de gestion"

    # Risques / Contrôles permanents / maîtrise des risques
    if "risque" in norm and ("controle" in norm or "contrôle" in norm or "permanent" in norm):
        return "Risques / Contrôles permanents"

    # Conformité / Sécurité financière
    if "conformite" in norm or "conformité" in norm or "securite financiere" in norm:
        return "Conformité / Sécurité financière"

    # Inspection / Audit
    if "inspection" in norm or "audit" in norm:
        return "Inspection / Audit"

    # Immobilier / bâtiments / sécurité des bâtiments
    if "immobilier" in norm or "batiment" in norm or "bâtiment" in norm or "securite et batiments" in norm:
        return "Immobilier"

    # Assurances
    if "assurance" in norm:
        return "Assurances"

    # Juridique / fiscalité / contentieux
    if "juridique" in norm or "fiscalite" in norm or "fiscalité" in norm or "contentieux" in norm:
        return "Juridique"

    # Marketing / Communication
    if "marketing" in norm or "communication" in norm:
        return "Marketing et Communication"

    # Organisation / Qualité
    if "organisation" in norm or "qualite" in norm or "qualité" in norm or "projets" in norm or "etudes" in norm or "études" in norm:
        # On laisse \"Analysesétudes et projets\" se regrouper ici
        return "Organisation / Qualité"

    # Gestion des opérations / support
    if "gestion des operations" in norm or "maitrise des operations" in norm or "support" in norm:
        return "Gestion des opérations"

    # Gestion d'actifs
    if "gestion d actifs" in norm or "gestion d'actifs" in norm or "gestion d actifs" in norm:
        return "Gestion d'Actifs"

    # Direction générale / management transverse
    if "direction generale" in norm or "management" in norm:
        return "Direction générale"

    # Développement durable / RSE
    if "developpement durable" in norm or "rse" in norm:
        return "Autres"

    # International
    if "international" in norm:
        return "Autres"

    # Achat
    if "achat" in norm:
        return "Autres"

    # Restauration / hôtellerie ou autres familles exotiques
    if "restauration" in norm or "hotellerie" in norm or "hôtellerie" in norm:
        return "Autres"

    return raw_family.strip()


def normalize_contract_type(raw_contract: str) -> str:
    """
    Normalise tous les types de contrat vers :
    Stage / Alternance / VIE / CDD / CDI / Graduate Programme.
    """
    if not raw_contract:
        return raw_contract
    s = str(raw_contract).strip()
    norm = _normalize_text(s)
    # Supprimer les parenthèses et leur contenu pour la comparaison (ex: "Working Student (Working Student)")
    s_clean = re.sub(r'\s*\([^)]*\)\s*', ' ', s).strip()
    norm_clean = _normalize_text(s_clean)

    # Stage
    if any(x in norm or x in norm_clean for x in [
        "stage", "internship", "auxiliaire de vacances", "job etudiant", "student job",
        "working student", "contrat etudiant", "stagesup"
    ]):
        return "Stage"

    # Alternance
    if any(x in norm or x in norm_clean for x in [
        "alternance", "apprentissage", "contrat de professionnalisation",
        "contrat d'apprentissage", "contrat en alternance", "contrat-dapprentissage",
        "contrat-de-professionnalisation", "contrat-en-alternance"
    ]):
        return "Alternance"

    # VIE
    if any(x in norm or x in norm_clean for x in ["vie", "v.i.e", "volontariat international"]):
        return "VIE"

    # CDD (Contractor / Temp / Fixed term = intérim / CDD)
    if any(x in norm or x in norm_clean for x in ["cdd", "temporary", "temporaire", "fixed term", "zero hours", "contractor", "temp"]):
        return "CDD"

    # Graduate Programme (CDI) → CDI (le contrat sous-jacent est CDI)
    if any(x in norm for x in ["graduate programme", "graduate program"]) and any(x in norm for x in ["cdi", "permanent"]):
        return "CDI"

    # Graduate Programme (sans précision CDI)
    if any(x in norm or x in norm_clean for x in ["graduate programme", "graduate program"]):
        return "Graduate Programme"

    # CDI
    if any(x in norm or x in norm_clean for x in ["cdi", "permanent", "reconversion professionnelle"]):
        return "CDI"

    return s


def normalize_experience_level(raw_exp: str) -> str:
    """
    Normalise les niveaux d'expérience pour n'exposer que :
    - '0 - 2 ans'
    - '3 - 5 ans'
    - '6 - 10 ans'
    - '11 ans et plus'
    Les libellés composés type 'Etudiant, Jeune diplômé, Junior, Confirmé' sont mappés.
    """
    if not raw_exp:
        return raw_exp

    norm = _normalize_text(raw_exp)

    # Mappages directs
    if "0 - 2 ans" in raw_exp:
        return "0 - 2 ans"
    if "3 - 5 ans" in raw_exp:
        return "3 - 5 ans"
    if "6 - 10 ans" in raw_exp:
        return "6 - 10 ans"
    if "11 ans et plus" in raw_exp or "11 ans" in raw_exp or "plus de 10 ans" in norm:
        return "11 ans et plus"

    # Libellés textuels (Étudiant / Jeune diplômé / Junior / Confirmé / Senior / Expert...)
    # On découpe sur virgules
    tokens = [t.strip() for t in re.split(r"[,/]", norm) if t.strip()]
    # Hiérarchie : Expert/Senior/Confirmé > Junior > Etudiant
    if any(t in ("expert", "senior") for t in tokens):
        return "11 ans et plus"
    if "confirme" in tokens or "confirmé" in norm:
        # Confirmé sans autre précision → 6-10 ans
        return "6 - 10 ans"
    if "junior" in tokens:
        return "0 - 2 ans"
    if "etudiant" in tokens or "etudiant" in norm or "jeune diplome" in norm:
        return "0 - 2 ans"

    return raw_exp.strip()

def fix_location(loc):
    """Corrige les locations incorrectes (ex: Tunis - France → Tunis - Tunisie, N/A - Luxembourg → Luxembourg).
    Normalise toujours le pays en sortie (ex: France, Royaume-Uni) pour cohérence site/filtres."""
    if not loc:
        return loc
    loc = loc.strip()
    loc = re.sub(r'\s*\(?\s*Ce\s+[Ll]ien\s+[Ss]\'[Oo]uvre\s+[Dd]ans\s+[Uu]n\s+[Nn]ouvel\s+[Oo]nglet\s*\)?', '', loc, flags=re.IGNORECASE).strip()
    if not loc:
        return loc
    if loc.upper().startswith('N/A') and (' - ' in loc or '-' in loc):
        parts = re.split(r'\s*-\s*', loc, maxsplit=1)
        if len(parts) >= 2 and parts[0].strip().upper() == 'N/A':
            return normalize_country(parts[1].strip())
    # Cas où la location est juste une ville (sans pays) : Tunis, Paris, Lyon, Clichy, Zurich...
    # On ajoute le pays pour éviter "Non spécifié / Autres" dans les filtres
    loc_lower = loc.lower().strip()
    if ' - ' not in loc:
        if 'grandcamp maisy' in loc_lower:
            return 'Grandcamp Maisy - France'
        if 'millénaire 4' in loc_lower or 'millenaire 4' in loc_lower:
            return loc.strip() + ' - France'
        if loc_lower == 'deutschlandweit':
            return normalize_country('Allemagne')
        if loc_lower in ('montréal', 'montreal'):
            return loc.strip() + ' - Canada'
        # Ville seule connue (Tunis→Tunisie, Paris/Lyon/Clichy→France, Zurich→Suisse)
        country = get_country_from_city(loc)
        if country:
            return f"{loc.strip()} - {normalize_country(country)}"
        # Pays seul (Allemagne, France, Luxembourg) → normaliser
        if loc_lower in ('allemagne', 'france', 'luxembourg', 'suisse', 'belgique'):
            return normalize_country(loc)
        return loc
    parts = loc.split(' - ', 1)
    city = (parts[0] or '').strip()
    country = (parts[1] or '').strip()
    if not country:
        return loc
    # Cas comme \"- - France\" ou \"- France\" → on garde uniquement le pays
    if not city or city == '-':
        return normalize_country(country)
    # Corriger pays erroné pour villes connues hors France (ex: Tunis - France → Tunisie, Bruxelles - France → Belgique)
    if country.lower() == 'france' and city:
        correct_country = get_country_from_city(city)
        if correct_country:
            return f"{city} - {normalize_country(correct_country)}"
    # Deutschlandweit = "partout en Allemagne" → garder uniquement le pays
    if city.lower().strip() == 'deutschlandweit' and country.lower() in ('allemagne', 'germany'):
        return normalize_country(country)
    # Toujours normaliser le nom du pays pour cohérence (France, Royaume-Uni, Taïwan, etc.)
    return f"{city} - {normalize_country(country)}"


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
            
            # Fallback publication_date si vide (ex: Deloitte ne fournit pas la date)
            if not job.get('publication_date') or not str(job.get('publication_date', '')).strip():
                first_seen = job.get('first_seen')
                if first_seen:
                    job['publication_date'] = str(first_seen)[:10]
            
            # Corriger les locations incorrectes (ex: Tunis - France → Tunis - Tunisie)
            if job.get('location'):
                job['location'] = fix_location(job['location'])

            # Normaliser le type de contrat (filet de sécurité)
            if job.get('contract_type'):
                job['contract_type'] = normalize_contract_type(job['contract_type'])
            
            # Normaliser la famille de métier pour éviter la prolifération de libellés exotiques
            if job.get('job_family'):
                job['job_family'] = normalize_job_family(job['job_family'])

            # Normaliser le niveau d'expérience pour rester sur 4 catégories canoniques
            if job.get('experience_level'):
                job['experience_level'] = normalize_experience_level(job['experience_level'])
            
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
            
            # Enrichir experience_level si vide : ré-extraction depuis description + fallback titre
            if not job.get('experience_level'):
                combined = " ".join(filter(None, [
                    job.get('job_description') or '',
                    job.get('company_description') or ''
                ]))
                extracted = extract_experience_level(
                    combined, job.get('contract_type'), job.get('job_title')
                )
                if extracted:
                    job['experience_level'] = normalize_experience_level(extracted)
            
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
        ("BPCE", BPCE_DB),
        ("Bpifrance", BPIFRANCE_DB),
        ("Crédit Mutuel", CREDIT_MUTUEL_DB),
        ("ODDO BHF", ODDO_BHF_DB),
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
