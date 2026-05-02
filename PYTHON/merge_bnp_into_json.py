#!/usr/bin/env python3
"""
Fusionne les offres BNP Paribas dans scraped_jobs_live.json SANS relancer les autres scrapers.
Utilisé par le workflow one-off BNP : scrape BNP → merge dans le JSON existant → commit.

Usage: python merge_bnp_into_json.py
"""

import json
import sqlite3
import sys
from pathlib import Path

# Import depuis export_sqlite_to_json pour réutiliser la logique de lecture
from export_sqlite_to_json import read_from_db, BNP_DB, HTML_DIR

OUTPUT_JSON = HTML_DIR / "scraped_jobs.json"
OUTPUT_JSON_LIVE = HTML_DIR / "scraped_jobs_live.json"


def get_bnp_company_names(db_path: Path) -> set:
    """Retourne l'ensemble des company_name présents dans la base BNP (toutes les marques)."""
    names = {"BNP Paribas"}  # Toujours inclure pour retirer l'ancien format
    if not db_path.exists():
        return names
    with sqlite3.connect(db_path) as conn:
        cursor = conn.execute(
            "SELECT DISTINCT company_name FROM jobs WHERE is_valid = 1 AND company_name IS NOT NULL AND company_name != ''"
        )
        names.update(row[0].strip() for row in cursor.fetchall() if row[0])
    return names


def main():
    if not BNP_DB.exists():
        print(f"❌ Base BNP manquante : {BNP_DB}")
        sys.exit(1)

    json_path = OUTPUT_JSON_LIVE
    if not json_path.exists():
        # Pas de JSON existant → on repart d'une liste vide (premier run ou JSON absent).
        # On ne bloque PAS : le delta scraping doit pouvoir avancer même sans JSON préalable.
        print(f"ℹ️  Fichier JSON absent ({json_path.name}) — création depuis zéro (OK pour un premier run).")
        existing_jobs = []
    else:
        # Charger le JSON existant
        with open(json_path, "r", encoding="utf-8") as f:
            existing_jobs = json.load(f)

    # Ensemble des marques BNP (Arval, BNP Paribas Cardif, etc.) pour retirer les anciennes
    bnp_names = get_bnp_company_names(BNP_DB)
    other_jobs = [j for j in existing_jobs if (j.get("company_name") or "").strip() not in bnp_names]
    count_removed = len(existing_jobs) - len(other_jobs)

    # Lire les offres BNP Live depuis la DB (même traitement que export_sqlite_to_json)
    bnp_jobs = read_from_db(BNP_DB, "BNP Paribas", live_only=True)

    # Fusionner
    merged = other_jobs + bnp_jobs

    # Sauvegarder
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(merged, f, ensure_ascii=False, indent=2)

    # Aussi scraped_jobs.json (version complète pour compatibilité)
    with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
        json.dump(merged, f, ensure_ascii=False, indent=2)

    print(f"✅ Fusion BNP terminée : {json_path.name}")
    print(f"   - Offres BNP retirées : {count_removed}")
    print(f"   - Offres BNP ajoutées : {len(bnp_jobs)}")
    print(f"   - Total : {len(merged)} offres Live")


if __name__ == "__main__":
    main()
