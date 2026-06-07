#!/usr/bin/env python3
"""Génère un résumé léger du catalogue Taleos pour le pilotage scraping."""

from __future__ import annotations

import json
import unicodedata
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
HTML_DIR = ROOT / "HTML"
INPUT_PATH = HTML_DIR / "scraped_jobs_live.json"

# Sources streaming individuelles (non incluses dans scraped_jobs_live.json)
# Clé = nom du fichier JSON (scraped_jobs_{key}.json), valeur = nom du groupe pour le summary
STREAMING_SOURCES = {
    "morgan_stanley":    "Morgan Stanley",
    "nomura":            "Nomura",
    "mufg":              "MUFG",
    "kepler_cheuvreux":  "Kepler Cheuvreux",
    "mizuho":            "Mizuho",
    "bank_of_america":   "Bank of America",
    "citi":              "Citi",
    "mazars":            "Mazars",
    "lazard":            "Lazard",
    "stripe":            "Stripe",
    "adyen":             "Adyen",
    "jefferies":         "Jefferies",
    "moelis":            "Moelis",
    "rothschild":        "Rothschild & Co",
    "accenture":         "Accenture",
}

OUTPUT_PATHS = [
    HTML_DIR / "scraped_jobs_summary.json",
    ROOT / "scraped_jobs_summary.json",
]


def normalize_company_group(company_name: str) -> str:
    raw = str(company_name or "").strip()
    if not raw:
        return "Non spécifié"

    normalized = unicodedata.normalize("NFD", raw.lower())
    normalized = "".join(ch for ch in normalized if unicodedata.category(ch) != "Mn")

    if any(token in normalized for token in [
        "credit agricole", "amundi", "caceis", "lcl", "indosuez", "bforbank", "uptevia", "idia"
    ]):
        return "Groupe Crédit Agricole"
    if any(token in normalized for token in [
        "bpce", "natixis", "caisse d epargne", "caisse d'epargne", "banque populaire", "credit cooperatif",
        "oney", "aew", "mirova", "ostrum", "banque palatine", "credit foncier",
        "casden", "capitole finance"
    ]):
        return "Groupe BPCE"
    if "societe generale" in normalized:
        return "Groupe Société Générale"
    if any(token in normalized for token in [
        "credit mutuel", "cic", "cofidis", "euro information", "banque transatlantique",
        "lyonnaise de banque", "afedim", "creatis", "factofrance", "monabanq"
    ]):
        return "Groupe Crédit Mutuel"
    if "bnp" in normalized:
        return "Groupe BNP Paribas"
    if "bpifrance" in normalized:
        return "Bpifrance"
    if "oddo" in normalized:
        return "ODDO BHF"
    if "deloitte" in normalized:
        return "Deloitte"
    if "jp morgan" in normalized or "jpmorgan" in normalized:
        return "J.P. Morgan"
    if "goldman sachs" in normalized:
        return "Goldman Sachs"
    return raw


def main() -> None:
    jobs = json.loads(INPUT_PATH.read_text(encoding="utf-8"))
    counter = Counter(
        normalize_company_group(job.get("company_name") or job.get("companyName"))
        for job in jobs
    )

    total = len(jobs)

    # Ajouter les sources streaming individuelles
    # Ces sources ne sont pas dans scraped_jobs_live.json mais dans des fichiers séparés
    for key, group_name in STREAMING_SOURCES.items():
        # Éviter le double-comptage : certaines sources (nomura, mufg, mizuho, bofa, citi)
        # sont maintenant incluses dans scraped_jobs_live.json via fallback.
        # On les inclut ici seulement si elles ne sont pas déjà comptées.
        if group_name in counter:
            continue
        json_path = HTML_DIR / f"scraped_jobs_{key}.json"
        if not json_path.exists():
            continue
        try:
            source_jobs = json.loads(json_path.read_text(encoding="utf-8"))
        except Exception:
            continue
        live_jobs = [j for j in source_jobs if j.get("status") != "Expired"]
        if live_jobs:
            counter[group_name] += len(live_jobs)
            total += len(live_jobs)
            print(f"   ✅ streaming {key} : {len(live_jobs)} offres ajoutées au summary")

    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "total_jobs": total,
        "counts_by_group": dict(sorted(counter.items())),
    }

    for output_path in OUTPUT_PATHS:
        output_path.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )

    print(f"Résumé écrit dans {', '.join(str(path) for path in OUTPUT_PATHS)}")
    print(f"Total : {total} offres ({len(counter)} groupes)")


if __name__ == "__main__":
    main()
