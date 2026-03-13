"""
Mapping des noms d'entreprises/filiales Crédit Mutuel (site) → nom d'affichage Taleos.
"""

# Raw (site) → Display (Taleos) - tout CM consolidé en "Crédit Mutuel" sauf marques distinctes
COMPANY_DISPLAY_MAPPING = {
    "cofidis": "Cofidis",
    "euro-information": "Euro Information",
    "euro-information developpements": "Euro Information",
    "euro information": "Euro Information",
    "cic": "CIC",
    "credit industriel et commercial": "CIC",
    "lyonnaise de banque": "Lyonnaise de Banque",
    "banque transatlantique": "Banque Transatlantique",
    "monabanq": "Monabanq",
    "creatis": "Creatis",
    "factofrance": "FactoFrance",
    "groupe la française": "Groupe La Française",
    "la française finance": "Groupe La Française",
    "afedim": "AFEDIM",
}

def normalize_company_name(raw: str) -> str:
    """
    Normalise le nom d'entreprise pour affichage.
    Ex: "EURO-INFORMATION DEVELOPPEMENTS" → "Euro Information"
    """
    if not raw or not str(raw).strip():
        return "Crédit Mutuel"
    key = str(raw).strip().lower()
    key_clean = key.replace("-", " ").replace("  ", " ")
    if key_clean in COMPANY_DISPLAY_MAPPING:
        return COMPANY_DISPLAY_MAPPING[key_clean]
    for k, display in COMPANY_DISPLAY_MAPPING.items():
        if k in key_clean or key_clean in k:
            return display
    if "credit industriel" in key_clean and "commercial" in key_clean:
        return "CIC"
    # Tout le reste du groupe CM (caisses, Factoring, Leasing, BECM, CCS, Synergie, etc.) → Crédit Mutuel
    if ("credit mutuel" in key_clean or "crédit mutuel" in key_clean or "ccm" in key_clean or
        "cmlaco" in key_clean or "becm" in key_clean or "ccs" in key_clean or "synergie" in key_clean or
        "caisse regionale" in key_clean or "caisse federale" in key_clean or "caisse de credit mutuel" in key_clean):
        return "Crédit Mutuel"
    return raw.strip()
