#!/usr/bin/env python3
import json
import subprocess
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from urllib.request import urlopen

ROOT = Path(__file__).parent
HTML_DIR = ROOT / "HTML"
LIVE_JSON = ROOT / "scraped_jobs_live.json"
HTML_LIVE_JSON = HTML_DIR / "scraped_jobs_live.json"
ROOT_SUMMARY = ROOT / "scraped_jobs_summary.json"
HTML_SUMMARY = HTML_DIR / "scraped_jobs_summary.json"


def load_jobs():
    candidates = []
    for path in (HTML_LIVE_JSON, LIVE_JSON):
        if path.exists():
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, list):
                candidates.append(data)

    public_head_sha = None
    try:
        out = subprocess.check_output(
            ["git", "ls-remote", "https://github.com/Dedale95/Taleos-Public.git", "refs/heads/main"],
            text=True,
            timeout=30,
        ).strip()
        if out:
            public_head_sha = out.split()[0]
    except Exception:
        public_head_sha = None

    remote_urls = []
    if public_head_sha:
        remote_urls.extend([
            f"https://raw.githubusercontent.com/Dedale95/Taleos-Public/{public_head_sha}/HTML/scraped_jobs_live.json",
            f"https://raw.githubusercontent.com/Dedale95/Taleos-Public/{public_head_sha}/scraped_jobs_live.json",
        ])
    remote_urls.extend([
        "https://raw.githubusercontent.com/Dedale95/Taleos-Public/main/HTML/scraped_jobs_live.json",
        "https://raw.githubusercontent.com/Dedale95/Taleos-Public/main/scraped_jobs_live.json",
    ])

    for url in remote_urls:
        try:
            with urlopen(url, timeout=60) as r:
                data = json.load(r)
            if isinstance(data, list):
                candidates.append(data)
        except Exception:
            continue

    if not candidates:
        raise FileNotFoundError("scraped_jobs_live.json introuvable")

    return max(candidates, key=len)


def is_axa(name: str) -> bool:
    n = (name or "").lower()
    return (
        n.startswith("axa")
        or any(s in n for s in [
            "direct assurance", "gie axa", "juridica",
            "mutuelle saint-christophe", "mutuelle saint christophe",
            "axa liabilities", "axa investment managers",
        ])
    )


def is_credit_agricole(name: str) -> bool:
    n = (name or "").lower()
    return (
        "crédit agricole" in n
        or "credit agricole" in n
        or any(s in n for s in [
            "lcl", "caceis", "amundi", "bforbank",
            "indosuez wealth management", "gruppo bancario crédit agricole italia",
            "idia capital investissement", "uptevia"
        ])
    )


def is_bpce(name: str) -> bool:
    n = (name or "").lower()
    return any(s in n for s in [
        "natixis", "caisse d'épargne", "caisse d'epargne", "banque populaire", "bpce",
        "crédit coopératif", "credit cooperatif", "banque palatine", "oney", "aew",
        "mirova", "ostrum", "crédit foncier", "credit foncier", "capitole finance", "casden"
    ])


def is_bnp(name: str) -> bool:
    n = (name or "").lower()
    return (
        "bnp paribas" in n
        or any(s in n for s in [
            "arval", "bgl bnp", "bnl", "teb", "hello bank",
            "banque commerciale en france", "nickel", "alfred berg"
        ])
    )


def is_credit_mutuel(name: str) -> bool:
    n = (name or "").lower()
    return (
        any(s in n for s in [
            "crédit mutuel", "credit mutuel", "cic", "cofidis", "euro information", "euro-information",
            "caisse fédérale", "caisse federale", "caisse regionale", "caisse régionale",
            "caisse de credit mutuel", "caisse de crédit mutuel", "becm", "bfcm", "monext", "acm", "acm gie",
            "assurances du credit mutuel", "lyonnaise de banque", "credit mutuel factoring",
            "credit mutuel leasing", "credit mutuel gestion", "credit mutuel asset management",
            "ccs", "synergie", "cmlaco", "banque transatlantique", "banque européenne",
            "banque europeenne", "creatis", "factofrance", "paysurf", "groupe la française", "groupe la francaise",
            "la française", "la francaise", "monabanq", "afedim", "credit mutuel caution",
            "credit mutuel amenagement", "confederation nationale", "ataraxia promotion"
        ])
        or n.startswith("ccm ")
        or n.endswith(" ccm")
    )


def canonical_group(name: str) -> str:
    n = (name or "").strip()
    low = n.lower()
    if not n:
        return "Non spécifié"
    if "goldman sachs" in low:
        return "Goldman Sachs"
    if "j.p. morgan" in low or "jp morgan" in low or "jpmorgan" in low:
        return "J.P. Morgan"
    if "société générale" in low or "societe generale" in low:
        return "Groupe Société Générale"
    if is_axa(n):
        return "AXA"
    if is_credit_agricole(n):
        return "Groupe Crédit Agricole"
    if is_bpce(n):
        return "Groupe BPCE"
    if is_bnp(n):
        return "Groupe BNP Paribas"
    if is_credit_mutuel(n):
        return "Groupe Crédit Mutuel"
    if "bpifrance" in low:
        return "Bpifrance"
    if "deloitte" in low:
        return "Deloitte"
    if "oddo" in low:
        return "ODDO BHF"
    if "la française" in low or "la francaise" in low:
        return "Groupe La Française"
    if "acm" in low:
        return "ACM GIE"
    if "arval" in low:
        return "Arval"
    if "ataraxia" in low:
        return "Ataraxia Promotion"
    if low == "bnl" or "banca nazionale del lavoro" in low:
        return "BNL"
    if "banque commerciale en france" in low:
        return "Banque Commerciale en France"
    if "hello bank" in low:
        return "Hello bank!"
    if "nickel" in low:
        return "Nickel"
    if low == "teb":
        return "TEB"
    return n


def main():
    jobs = load_jobs()
    grouped = Counter(canonical_group(job.get("company_name") or "") for job in jobs)

    # Fraîcheur par groupe : date de la dernière offre ajoutée/mise à jour
    # On prend le max de first_seen et last_updated pour chaque offre, par groupe.
    freshness: dict[str, str] = {}
    for job in jobs:
        group = canonical_group(job.get("company_name") or "")
        # Prendre la date la plus récente disponible sur l'offre
        dates = [d for d in [job.get("first_seen"), job.get("last_updated")] if d]
        if not dates:
            continue
        job_latest = max(dates)
        if group not in freshness or job_latest > freshness[group]:
            freshness[group] = job_latest

    summary = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "total_jobs": len(jobs),
        "counts_by_group": dict(sorted(grouped.items(), key=lambda item: (-item[1], item[0].lower()))),
        # dernière date d'ajout/mise à jour d'une offre, par groupe (ISO 8601)
        "freshness_by_group": dict(sorted(freshness.items())),
    }
    for path in (ROOT_SUMMARY, HTML_SUMMARY):
        with open(path, "w", encoding="utf-8") as f:
            json.dump(summary, f, ensure_ascii=False, indent=2)
            f.write("\n")
    print(f"Résumé généré: {len(jobs)} offres, {len(grouped)} groupes")


if __name__ == "__main__":
    main()
