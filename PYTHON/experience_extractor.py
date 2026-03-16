#!/usr/bin/env python3
"""
Module partagé pour extraire le niveau d'expérience requis depuis le détail des offres.
Utilisé par les scrapers BPCE, Bpifrance, BNP Paribas, Société Générale, Deloitte, etc.
Format de sortie harmonisé : "0 - 2 ans", "3 - 5 ans", "6 - 10 ans", "11 ans et plus"
"""

import re
from typing import Optional


def extract_experience_level(
    text: str,
    contract_type: Optional[str] = None,
    job_title: Optional[str] = None
) -> Optional[str]:
    """
    Extrait le niveau d'expérience attendu depuis le texte de l'offre (description, company_description, etc.).
    Fallback : infère depuis le titre du poste si la description ne donne rien.
    
    Args:
        text: Texte complet de l'offre (description, company_description, etc.)
        contract_type: Type de contrat (Stage, VIE, Alternance → toujours 0-2 ans)
        job_title: Titre du poste (fallback pour inférence : Senior, Junior, Lead, Manager, etc.)
    
    Returns:
        "0 - 2 ans", "3 - 5 ans", "6 - 10 ans", "11 ans et plus" ou None
    """
    # Règle prioritaire : Stage, VIE, Alternance → toujours 0-2 ans
    if contract_type and str(contract_type).strip():
        ct_lower = contract_type.lower()
        if any(x in ct_lower for x in ['stage', 'vie', 'alternance', 'apprentissage', 'intern', 'trainee']):
            return "0 - 2 ans"
    
    text_lower = (text or "").lower().strip()
    if not text_lower and not job_title:
        return None
    
    # 1. "Niveau d'expérience minimum X - Y ans" (format Crédit Agricole, BPCE)
    niv_min = re.search(
        r"niveau\s*d['\u2019]expérience\s*(?:minimum|requis)?\s*:?\s*(\d+)\s*[-–]\s*(\d+)\s*ans",
        text_lower,
        re.IGNORECASE
    )
    if niv_min:
        low, high = int(niv_min.group(1)), int(niv_min.group(2))
        return _years_to_level(low, high)
    
    # 2. "X ans d'expérience" / "X ans d expérience" (priorité pour précision)
    years_m = re.search(r"(\d+)\s*ans\s*d['\u2019\s]expérience", text_lower)
    if years_m:
        y = int(years_m.group(1))
        if y <= 2:
            return "0 - 2 ans"
        if y <= 5:
            return "3 - 5 ans"
        if y <= 10:
            return "6 - 10 ans"
        return "11 ans et plus"
    
    # 3. "X à Y ans" / "X - Y ans" / "X to Y years" (FR + EN)
    range_m = re.search(
        r"(\d+)\s*[-–àto]\s*(\d+)\s*(?:ans|years?)(?:\s*(?:of\s*)?experience)?",
        text_lower
    )
    if range_m:
        low, high = int(range_m.group(1)), int(range_m.group(2))
        return _years_to_level(low, high)
    
    # 4. "between X and Y years"
    between_m = re.search(r"between\s*(\d+)\s*and\s*(\d+)\s*years?", text_lower)
    if between_m:
        low, high = int(between_m.group(1)), int(between_m.group(2))
        return _years_to_level(low, high)
    
    # 5. "minimum X ans" / "min. X ans" / "X ans minimum" / "minimum X years"
    min_m = re.search(r"min(?:imum|\.)?\s*(\d+)\s*(?:ans|years?)|(\d+)\s*ans\s*min(?:imum|\.)?", text_lower)
    if min_m:
        y = int(min_m.group(1) or min_m.group(2) or 0)
        if y <= 2:
            return "0 - 2 ans"
        if y <= 5:
            return "3 - 5 ans"
        if y <= 10:
            return "6 - 10 ans"
        return "11 ans et plus"
    
    # 5a. "minimum de deux/trois/... ans" (mots français)
    fr_numbers = {'deux': 2, 'trois': 3, 'quatre': 4, 'cinq': 5, 'six': 6, 'sept': 7, 'huit': 8, 'neuf': 9, 'dix': 10}
    min_fr = re.search(r"minimum\s+de\s+(deux|trois|quatre|cinq|six|sept|huit|neuf|dix)\s*ans", text_lower)
    if min_fr:
        y = fr_numbers.get(min_fr.group(1), 2)
        if y <= 2:
            return "0 - 2 ans"
        if y <= 5:
            return "3 - 5 ans"
        if y <= 10:
            return "6 - 10 ans"
        return "11 ans et plus"
    
    # 5b. "plus de X ans" / "more than X years" (générique)
    plus_de_m = re.search(r"(?:plus\s+de|more\s+than|over)\s*(\d+)\s*(?:ans|years?)", text_lower)
    if plus_de_m:
        y = int(plus_de_m.group(1))
        if y <= 2:
            return "0 - 2 ans"
        if y <= 5:
            return "3 - 5 ans"
        if y <= 10:
            return "6 - 10 ans"
        return "11 ans et plus"
    
    # 5c. "X ans et plus" / "X years and more"
    ans_et_plus = re.search(r"(\d+)\s*(?:ans|years?)\s*(?:et\s+plus|and\s+more|\+)", text_lower)
    if ans_et_plus:
        y = int(ans_et_plus.group(1))
        if y <= 2:
            return "0 - 2 ans"
        if y <= 5:
            return "3 - 5 ans"
        if y <= 10:
            return "6 - 10 ans"
        return "11 ans et plus"
    
    # 5d. "at least X years" / "au moins X ans"
    at_least_m = re.search(r"(?:at\s+least|au\s+moins)\s+(\d+)\s*(?:ans|years?)", text_lower)
    if at_least_m:
        y = int(at_least_m.group(1))
        if y <= 2:
            return "0 - 2 ans"
        if y <= 5:
            return "3 - 5 ans"
        if y <= 10:
            return "6 - 10 ans"
        return "11 ans et plus"
    
    # 5d1. "au moins X mois" (convertir en ans)
    at_least_mois = re.search(r"(?:at\s+least|au\s+moins)\s+(\d+)\s*mois", text_lower)
    if at_least_mois:
        mois = int(at_least_mois.group(1))
        y = max(1, (mois + 11) // 12)
        if y <= 2:
            return "0 - 2 ans"
        if y <= 5:
            return "3 - 5 ans"
        return "6 - 10 ans"
    
    # 5d2. "X mois d'expérience"
    mois_m = re.search(r"(\d+)\s*mois\s*d['\u2019\s]expérience", text_lower)
    if mois_m:
        mois = int(mois_m.group(1))
        y = (mois + 11) // 12
        if y <= 2:
            return "0 - 2 ans"
        if y <= 5:
            return "3 - 5 ans"
        if y <= 10:
            return "6 - 10 ans"
        return "11 ans et plus"
    
    # 5d3. "expérience de X ans ou plus" / "X ans ou plus"
    ans_ou_plus = re.search(r"(?:expérience\s+de\s+)?(\d+)\s*ans\s*ou\s*plus", text_lower)
    if ans_ou_plus:
        y = int(ans_ou_plus.group(1))
        if y <= 2:
            return "0 - 2 ans"
        if y <= 5:
            return "3 - 5 ans"
        if y <= 10:
            return "6 - 10 ans"
        return "11 ans et plus"
    
    # 5e. "X years of experience" / "X+ years" (Required: 8+ years, 8+ years prior ... experience)
    # Inclut "8+ years prior compliance advisory experience" (texte entre years et experience)
    # Prendre le max si plusieurs mentions. 10+ = senior → "11 ans et plus"
    years_exp_all = re.findall(r"(\d+)\+\s*years?\s*(?:of\s*)?experience", text_lower)
    years_exp_prior = re.findall(r"(\d+)\+\s*years?\s+[^.]*?experience", text_lower)
    years_exp_all = list(set(years_exp_all + years_exp_prior))
    if years_exp_all:
        y = max(int(x) for x in years_exp_all)
        if y <= 2:
            return "0 - 2 ans"
        if y <= 5:
            return "3 - 5 ans"
        if y >= 10:
            return "11 ans et plus"  # 10+ = senior
        return "6 - 10 ans"
    years_exp_m = re.search(r"(\d+)\s*years?\s*(?:of\s*)?experience", text_lower)
    if years_exp_m:
        y = int(years_exp_m.group(1))
        if y <= 2:
            return "0 - 2 ans"
        if y <= 5:
            return "3 - 5 ans"
        if y <= 10:
            return "6 - 10 ans"
        return "11 ans et plus"
    
    # 6. Patterns textuels (ordre : plus spécifique → plus générique)
    patterns = [
        (r'(?:plus de|more than|over)\s*(?:10|11|15|20)\s*(?:ans|years?)', "11 ans et plus"),
        (r'(?:10|11|12|13|14|15)\+?\s*(?:ans|years?)', "11 ans et plus"),
        (r'senior\s+manager|director|\bexpert[s]?\s+(?:en|dans|in|immobilier)', "11 ans et plus"),
        (r'lead\s+(?:analyst|developer|engineer|manager)', "6 - 10 ans"),
        (r'principal\s+(?:engineer|consultant|analyst)', "11 ans et plus"),
        (r'\bsenior\b|\bconfirmé\b|confirmed|\bexpert[s]?\b|\bexpérimenté[s]?\b', "11 ans et plus"),
        (r'(?:6|7|8|9|10)\s*(?:-|à|to)\s*(?:10|11|12)\s*(?:ans|years?)', "6 - 10 ans"),
        (r'(?:5|6|7|8|9|10)\+?\s*(?:ans|years?)', "6 - 10 ans"),
        (r'(?:3|4|5)\s*(?:-|à|to)\s*(?:5|6|7)\s*(?:ans|years?)', "3 - 5 ans"),
        (r'(?:2|3|4)\s*(?:-|à|to)\s*(?:4|5)\s*(?:ans|years?)', "3 - 5 ans"),
        (r'(?:0|1|2)\s*(?:-|à|to)\s*(?:2|3)\s*(?:ans|years?)', "0 - 2 ans"),
        (r'junior|débutant|beginner|entry|jeune diplômé|stagiaire|alternant', "0 - 2 ans"),
        (r'recent\s+graduate|young\s+graduate|graduate\s+program', "0 - 2 ans"),
        (r'first\s+experience|première expérience|premier poste|première expérience réussie', "0 - 2 ans"),
        (r'expérience\s+réussie|premières?\s+expériences?\s+professionnelles?', "0 - 2 ans"),
        (r'aucune\s+expérience\s+requise|no\s+experience\s+required', "0 - 2 ans"),
        (r'expérience\s+(?:requise|souhaitée|attendue)|experience\s+(?:required|needed)', "3 - 5 ans"),
        (r'(?:relevant|demonstrated|proven)\s+experience', "3 - 5 ans"),
        (r'early\s+career|entry\s*level', "0 - 2 ans"),
        (r'less than 2|moins de 2|moins de deux', "0 - 2 ans"),
    ]
    for pattern, level in patterns:
        if re.search(pattern, text_lower):
            return level
    
    # Fallback : inférer depuis le titre du poste (pour nouvelles offres et offres sans mention explicite)
    if job_title:
        title_lower = job_title.lower()
        if any(x in title_lower for x in ['stage', 'alternance', 'vie', 'intern', 'stagiare']):
            return "0 - 2 ans"
        if any(x in title_lower for x in ['junior', 'graduate', 'jeune diplômé']):
            return "0 - 2 ans"
        if any(x in title_lower for x in ['senior', 'director', 'expert', 'chief']):
            return "11 ans et plus"
        if 'lead' in title_lower or 'principal' in title_lower:
            return "6 - 10 ans"
        if 'manager' in title_lower or 'confirmé' in title_lower:
            return "6 - 10 ans"
    
    return None


def _years_to_level(low: int, high: int) -> str:
    """Mappe une plage d'années à notre format standard."""
    if high <= 2:
        return "0 - 2 ans"
    if high <= 5:
        return "3 - 5 ans"
    if high <= 10:
        return "6 - 10 ans"
    return "11 ans et plus"
