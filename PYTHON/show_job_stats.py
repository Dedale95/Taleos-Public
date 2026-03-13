#!/usr/bin/env python3
"""
Affiche le récapitulatif Live / Expired par source.
Usage: python show_job_stats.py
"""

import sqlite3
from pathlib import Path
from datetime import datetime

PYTHON_DIR = Path(__file__).parent
CA_DB = PYTHON_DIR / "credit_agricole_jobs.db"
SG_DB = PYTHON_DIR / "societe_generale_jobs.db"
DELOITTE_DB = PYTHON_DIR / "deloitte_jobs.db"
BNP_DB = PYTHON_DIR / "bnp_paribas_jobs.db"

def main():
    print("=" * 55)
    print("📊 OFFRES PAR SOURCE (Live / Expired)")
    print("=" * 55)
    print(f"Date : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()

    sources = [
        ("Crédit Agricole", CA_DB),
        ("Société Générale", SG_DB),
        ("Deloitte", DELOITTE_DB),
        ("BNP Paribas", BNP_DB),
    ]

    total_live = 0
    total_expired = 0

    for name, db_path in sources:
        if not db_path.exists():
            print(f"   {name}: base manquante")
            continue
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
        print(f"   {name}: {live} Live | {expired} Expired")

    print("-" * 55)
    print(f"   Total Live (exporté): {total_live}")
    print(f"   Total Expired:        {total_expired}")
    print("=" * 55)

if __name__ == "__main__":
    main()
