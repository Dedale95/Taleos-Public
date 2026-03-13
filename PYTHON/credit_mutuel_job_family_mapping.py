"""
Mapping des familles de métier Crédit Mutuel → catégories Taleos existantes.
Harmonise avec job_family_classifier et offres.html familySynonyms.
"""

# Crédit Mutuel (site) → Taleos (job_family_classifier / offres.html)
CREDIT_MUTUEL_TO_TALEOS = {
    # Correspondances directes
    "comptabilité et finance": "Finances / Comptabilité / Contrôle de gestion",
    "risque, contrôle et conformité": "Risques / Contrôles permanents",
    "ressources humaines": "Ressources Humaines",
    "relation client": "Commercial / Relations Clients",
    "marketing et communication": "Marketing et Communication",
    "maîtrise des opérations et support": "Gestion des opérations",
    "maitrise des operations et support": "Gestion des opérations",
    "logistique et achats": "Achat",
    "juridique et fiscalité": "Juridique",
    "inspection et audit": "Inspection / Audit",
    "informatique, technologies et data": "IT, Digital et Data",
    "expertise commercial et accompagnement réseau": "Commercial / Relations Clients",
    "banque de financement et d'investissement et gestion d'actifs": "Financement et Investissement",
    "analyses, études et projets": "Analyse financière et économique",
    "analyses, etudes et projets": "Analyse financière et économique",
    "engagement, recouvrement et contentieux": "Juridique",
    # Conformité séparée (risque+conformité peut couvrir les deux)
    "rse-esg": "Autres",
    "restauration, hôtellerie": "Autres",
    "restauration, hotellerie": "Autres",
    "immobilier, sécurité et bâtiments": "Autres",
    "immobilier, securite et batiments": "Autres",
    "management": "Autres",
    "assurance": "Autres",
}


def map_credit_mutuel_family(raw: str) -> str:
    """
    Mappe une famille de métier Crédit Mutuel vers une catégorie Taleos.
    
    Args:
        raw: Famille brute du site CM (ex: "Comptabilité et finance")
        
    Returns:
        Famille harmonisée Taleos ou "Autres" si non mappée
    """
    if not raw or not str(raw).strip():
        return "Autres"
    key = str(raw).strip().lower()
    return CREDIT_MUTUEL_TO_TALEOS.get(key, raw.strip())
