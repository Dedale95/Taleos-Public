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
    
    # 5d4. Allemand : "X Jahre Berufserfahrung" / "X Jahre Erfahrung" / "X Jahren"
    jahre_m = re.search(r"(\d+)\s*(?:bis\s*\d+\s*)?jahre\w*\s*(?:berufs)?erfahrung", text_lower)
    if jahre_m:
        y = int(jahre_m.group(1))
        if y <= 2:  return "0 - 2 ans"
        if y <= 5:  return "3 - 5 ans"
        if y <= 10: return "6 - 10 ans"
        return "11 ans et plus"

    jahre_m2 = re.search(r"(\d+)\+?\s*jahre\w*\s*erfahrung", text_lower)
    if jahre_m2:
        y = int(jahre_m2.group(1))
        if y <= 2:  return "0 - 2 ans"
        if y <= 5:  return "3 - 5 ans"
        if y <= 10: return "6 - 10 ans"
        return "11 ans et plus"

    # 5d5. Portugais / Espagnol : "X años de experiencia" / "X anos de experiência"
    anos_m = re.search(r"(\d+)\s*a[nñ]os?\s*de\s*experi[eê]ncia", text_lower)
    if anos_m:
        y = int(anos_m.group(1))
        if y <= 2:  return "0 - 2 ans"
        if y <= 5:  return "3 - 5 ans"
        if y <= 10: return "6 - 10 ans"
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
        (r'senior\s+(?:vice\s*president|director|manager)', "11 ans et plus"),
        (r'lead\s+(?:analyst|developer|engineer|manager)', "6 - 10 ans"),
        (r'principal\s+(?:engineer|consultant|analyst)', "11 ans et plus"),
        (r'\bexpert[s]?\s+(?:en|dans|in|immobilier)|\bexpérimenté[s]?\b|\bconfirmé[s]?\b|confirmed', "6 - 10 ans"),
        (r'(?:6|7|8|9|10)\s*(?:-|à|to)\s*(?:10|11|12)\s*(?:ans|years?)', "6 - 10 ans"),
        (r'(?:5|6|7|8|9|10)\+?\s*(?:ans|years?)', "6 - 10 ans"),
        (r'(?:3|4|5)\s*(?:-|à|to)\s*(?:5|6|7)\s*(?:ans|years?)', "3 - 5 ans"),
        (r'(?:2|3|4)\s*(?:-|à|to)\s*(?:4|5)\s*(?:ans|years?)', "3 - 5 ans"),
        (r'(?:0|1|2)\s*(?:-|à|to)\s*(?:2|3)\s*(?:ans|years?)', "0 - 2 ans"),
        (r'junior|débutant|beginner|jeune diplômé|stagiaire|alternant', "0 - 2 ans"),
        (r'recent\s+graduate|young\s+graduate|graduate\s+program', "0 - 2 ans"),
        (r'first\s+experience|première expérience|premier poste|première expérience réussie', "0 - 2 ans"),
        (r'expérience\s+réussie|premières?\s+expériences?\s+professionnelles?', "0 - 2 ans"),
        (r'aucune\s+expérience\s+requise|no\s+experience\s+required', "0 - 2 ans"),
        (r'expérience\s+(?:requise|souhaitée|attendue)|experience\s+(?:required|needed)', "3 - 5 ans"),
        (r'(?:relevant|demonstrated|proven)\s+experience', "3 - 5 ans"),
        (r'early\s+career|entry\s*level', "0 - 2 ans"),
        (r'less than 2|moins de 2|moins de deux', "0 - 2 ans"),
        # Allemand
        (r'duales?\s+studium|duale\s+ausbildung|berufsausbildung', "0 - 2 ans"),
        (r'berufseinstieg|einsteiger|berufseinsteiger', "0 - 2 ans"),
        (r'mehrj[äa]hrige\s+berufserfahrung|langjährige\s+erfahrung', "6 - 10 ans"),
        (r'fundierte\s+(?:kenntnisse|erfahrung)', "3 - 5 ans"),
        (r'erste\s+berufserfahrung|erste\s+erfahrungen?', "0 - 2 ans"),
        # Espagnol / Portugais
        (r'primer\s+empleo|sin\s+experiencia|reci[eé]n\s+graduad', "0 - 2 ans"),
        (r'experiencia\s+comprobada|experiência\s+comprovad', "3 - 5 ans"),
    ]
    for pattern, level in patterns:
        if re.search(pattern, text_lower):
            return level

    # Fallback : inférer depuis le titre du poste
    # Hiérarchie finance : du plus senior au plus junior (ordre important — vérifier d'abord les titres composés)
    return _infer_from_title(job_title)


def _infer_from_title(job_title: Optional[str]) -> Optional[str]:
    """
    Infère le niveau d'expérience depuis le titre du poste.
    Hiérarchie finance internationale (JP Morgan, BNP, SG, Goldman Sachs…) :
      11 ans+ : Managing Director, Executive Director, Chief, Head of, Senior VP/SVP
       6-10   : Vice President, VP, Director, Senior, Lead, Principal, Manager, Head
       3-5    : Associate, Analyst, Graduate, Officer, Specialist, Consultant, Chargé, Conseiller, Banker
       0-2    : Stage, Alternance, Intern, Trainee, Praktikum, Junior
    """
    if not job_title:
        return None
    t = job_title.lower()

    # ── 11 ans et plus ────────────────────────────────────────────────────────
    # Titres composés en premier (sinon "vice president" matcher sur "president")
    if re.search(r'\bmanaging\s*director\b|\bmd\b(?:\s|$)', t): return "11 ans et plus"
    if re.search(r'\bexecutive\s*director\b|\bed\b(?:\s|$)', t): return "11 ans et plus"
    if re.search(r'\bsenior\s*(?:vice\s*president|vp|managing|director)\b', t): return "11 ans et plus"
    if re.search(r'\b(?:chief|ceo|cfo|cto|cio|cdo|coo|cro)\b', t): return "11 ans et plus"
    if re.search(r'\bhead\s+of\b|\bdirecteur\s+(?:général|exécutif|financier|des\s+\w+)\b', t): return "11 ans et plus"
    if re.search(r'\bpartner\b|\bassocié[e]?\s+(?:principal|senior|gérant)\b', t): return "11 ans et plus"
    if re.search(r'\bpresident\b(?!.*vice)', t): return "11 ans et plus"  # President mais pas Vice President

    # ── 6 - 10 ans ────────────────────────────────────────────────────────────
    # AVP (Assistant VP) = 3-5 chez BNP/HSBC/SG, mais on traite AVANT le check VP
    if re.search(r'\bavp\b', t): return "3 - 5 ans"
    # VP peut être suivi de virgule, deux-points, tiret, espace, fin de chaîne
    if re.search(r'\bvice\s*president\b|\bvp\b(?:\s|[-–,:]|$)', t): return "6 - 10 ans"
    if re.search(r'\b(?:senior|sr\.?)\b', t): return "6 - 10 ans"
    if re.search(r'\bdirector\b|\bdirecteur\b|\bdirekteur\b|\bdirettore\b|\bdirector[a]?\b', t): return "6 - 10 ans"
    if re.search(r'\blead\b|\bleader\b|\bleitung\b', t): return "6 - 10 ans"
    if re.search(r'\bprincipal\b', t): return "6 - 10 ans"
    if re.search(r'\bmanager\b|\bresponsable\b|\bverantwoordelijke\b|\bleiter\b|\bgestionnaire\b', t): return "6 - 10 ans"
    if re.search(r'\bgerente\b|\bjefe\b|\bdirigentem\b|\bkierownik\b|\bkierowniczk\b', t): return "6 - 10 ans"  # ES/PL
    if re.search(r'\bhead\b', t): return "6 - 10 ans"
    if re.search(r'\barchitect[e]?\b|\barchitekten?\b|\barquitecto\b', t): return "6 - 10 ans"
    if re.search(r'\bexperte?\b|\bexpert[se]?\b|\bexpérimenté[e]?\b|\bconfirmé[e]?\b', t): return "6 - 10 ans"
    if re.search(r'\bstarszy\b|\bстарший\b|\bстарша\b', t): return "6 - 10 ans"  # PL/UA "senior"

    # ── 3 - 5 ans ────────────────────────────────────────────────────────────
    if re.search(r'\bassociat[e]?\b|\bassocié[e]?\b', t): return "3 - 5 ans"
    if re.search(r'\banalyst[e]?\b|\banalytiker\b|\banalista\b|\banalityk\b', t): return "3 - 5 ans"
    if re.search(r'\bgraduate\b|\bgraduat[e]?\b', t): return "3 - 5 ans"
    if re.search(r'\bofficer\b|\boficier\b|\bbeauftragte[rn]?\b', t): return "3 - 5 ans"
    if re.search(r'\bspecialist[e]?\b|\bspécialiste\b|\bspezialist\b|\bespecialista\b|\bspecjalista\b', t): return "3 - 5 ans"
    if re.search(r'\bconsultant[e]?\b|\bkonsultant\b|\bconsultor[a]?\b|\bberater\b|\bkonsulent\b', t): return "3 - 5 ans"
    if re.search(r'\bcharg[eé][e]?\b|\bgestionnaire\s+de\b|\bchef\s+de\s+(?:projet|produit)\b', t): return "3 - 5 ans"
    # Advisor / Counsel : anglais, français, polonais, ukrainien
    if re.search(r'\bconseiller\b|\bconseillere?\b|\badvisor\b|\bcounsel\b|\bdoradca\b|\bdoradczyni\b|\bdoradcz\b', t): return "3 - 5 ans"
    if re.search(r'\bbanker\b|\bbanquier\b|\brelationship\s+\w*\s*manager\b', t): return "3 - 5 ans"
    if re.search(r'\bengineer\b|\bingénieur\b|\bingenieur\b|\bengineering\b|\bingeniería\b', t): return "3 - 5 ans"
    if re.search(r'\bdeveloper\b|\bdéveloppeur\b|\bdeveloppeur\b|\bentwickler\b|\bdesarrollador\b', t): return "3 - 5 ans"
    if re.search(r'\bcontroller\b|\bcontrôleur\b|\bcontrolleur\b|\bcontrolador\b', t): return "3 - 5 ans"
    if re.search(r'\bauditor\b|\bauditeur\b|\bprüfer\b|\bauditor[e]?\b', t): return "3 - 5 ans"
    if re.search(r'\btrader\b|\bportfolio\s+manager\b', t): return "3 - 5 ans"
    if re.search(r'\bcoordinat[eo]r?\b|\bcoordinateu?r?\b|\bkoordinator\b|\bcoordinador\b', t): return "3 - 5 ans"
    if re.search(r'\badministrat[eo]r?\b|\badministrateur\b|\badministrador\b', t): return "3 - 5 ans"
    if re.search(r'\btecnico\b|\btécnico\b|\btecnica\b|\boperatore\b|\btechnician\b|\btechnicien\b', t): return "3 - 5 ans"  # ES/IT/FR
    if re.search(r'\bgestor[a]?\b|\bcomercial\b|\bexecutiv[oa]\b', t): return "3 - 5 ans"  # ES
    if re.search(r'\bassistant\b|\bassistante\b|\bassistentin\b|\bassistente\b', t): return "3 - 5 ans"
    if re.search(r'\bscrum\s+master\b|\bproduct\s+owner\b|\bproject\s+manager\b', t): return "3 - 5 ans"
    if re.search(r'\btester\b|\btesteuse?\b|\bqa\b|\bquality\s+assurance\b', t): return "3 - 5 ans"
    if re.search(r'\bscientist\b|\bscientifique\b|\bwissenschaftler\b', t): return "3 - 5 ans"
    if re.search(r'\baccountant\b|\bcomptable\b|\bbuchhalter\b|\bcontable\b', t): return "3 - 5 ans"
    if re.search(r'\binformaticien\b|\binformatiker\b|\binformatico\b', t): return "3 - 5 ans"
    if re.search(r'\bfachmann\b|\bfachfrau\b|\bfachkraft\b|\bfachspezialist\b', t): return "3 - 5 ans"  # DE
    if re.search(r'\boperations?\b|\bopérateur\b', t): return "3 - 5 ans"
    if re.search(r'\bчарівник\b|\bфахівець\b|\bспеціаліст\b|\bконсультант\b', t): return "3 - 5 ans"  # UA
    if re.search(r'\bголовний\b|\bначальник\b', t): return "6 - 10 ans"  # UA "head/chief"
    # Rôles bancaires FR sans titre hiérarchique explicite (BPCE, Crédit Mutuel…)
    if re.search(r'\bjuriste\b|\bparalegal\b|\battorney\b|\blawyer\b', t): return "3 - 5 ans"
    if re.search(r'\banimat(eur|rice|eurs|rices)\b', t): return "3 - 5 ans"
    if re.search(r'\battach[eé][e]?\b', t): return "3 - 5 ans"
    if re.search(r'\bformat(eur|rice)\b|\bformateur\b', t): return "3 - 5 ans"
    if re.search(r'\bdélégu[eé][e]?\b|\bdelegu[eé][e]?\b', t): return "3 - 5 ans"
    if re.search(r'\bagent\b', t): return "3 - 5 ans"
    if re.search(r'\bactuaire\b|\bactuary\b|\bactuarial\b', t): return "3 - 5 ans"
    if re.search(r'\bmodélisat(eur|rice)\b|\bmodelisateur\b', t): return "3 - 5 ans"
    if re.search(r'\bquant\b|\bquantitative\b|\bquantitatif\b', t): return "3 - 5 ans"
    if re.search(r'\bsuperviseur\b|\bsupervisor\b|\bsupervizor\b', t): return "6 - 10 ans"  # FR/EN/RO
    if re.search(r'\binspect(eur|rice)\b|\binspector\b|\binspecteur\b', t): return "6 - 10 ans"
    if re.search(r'\bcommercial[e]?\b', t): return "3 - 5 ans"  # FR (≠ ES "comercial")
    if re.search(r'\bdevops\b|\bdevsecops\b|\bsre\b', t): return "3 - 5 ans"
    if re.search(r'\btéléconseiller\b|\bteleconseiller\b|\btéléconseil\b', t): return "3 - 5 ans"
    if re.search(r'\borganisateur\b|\borganisatrice\b', t): return "3 - 5 ans"
    if re.search(r'\brecruiter\b|\brecruteur\b|\brecruteuse\b', t): return "3 - 5 ans"
    if re.search(r'\bgeneraliste?\b|\bgeneralist\b', t): return "3 - 5 ans"
    if re.search(r'\bmandataire\b', t): return "3 - 5 ans"
    if re.search(r'\brepresentative\b|\brepr[eé]sentant[e]?\b', t): return "3 - 5 ans"
    if re.search(r'\breceptionist\b|\bh[ôo]tesse\b|\baccueil\b', t): return "3 - 5 ans"
    if re.search(r'\bchef\s+de\s+mission\b', t): return "6 - 10 ans"
    if re.search(r'\bchef\s+(?:de\s+)?(?:projet|produit|service|département|groupe)\b', t): return "6 - 10 ans"
    if re.search(r'\brelationship\s+manager\b|\brelationship\s+banker\b', t): return "6 - 10 ans"
    if re.search(r'\bportfolio\s+manager\b|\bfund\s+manager\b', t): return "6 - 10 ans"
    if re.search(r'\bproduct\s+manager\b|\bproduct\s+owner\b', t): return "3 - 5 ans"
    if re.search(r'\bprogram\s+manager\b|\bprogramme\s+manager\b', t): return "6 - 10 ans"
    # Compétences IT sans hiérarchie : par défaut 3-5 ans
    if re.search(r'\bengineering\s+manager\b|\btechnical\s+(?:lead|manager)\b', t): return "6 - 10 ans"
    if re.search(r'\bdata\s+(?:scientist|engineer|analyst|manager)\b', t): return "3 - 5 ans"
    if re.search(r'\bcloud\s+(?:architect|engineer)\b|\bsolutions?\s+architect\b', t): return "6 - 10 ans"
    # Allemand : titres non couverts
    if re.search(r'\bsachbearbeiter\w*\b|\bsachbearbeitung\b', t): return "3 - 5 ans"  # DE "collaborateur"
    if re.search(r'\bauszubildende\w*\b|\bausbildung\b', t): return "0 - 2 ans"  # DE apprentissage
    if re.search(r'\bwerkstudent\w*\b|\bpraktikant\w*\b', t): return "0 - 2 ans"  # DE étudiant/stagiaire
    if re.search(r'\breferent\w*\b|\breferentin\b', t): return "3 - 5 ans"  # DE spécialiste
    if re.search(r'\bfachberater\w*\b|\bkundenberater\w*\b', t): return "3 - 5 ans"  # DE conseiller

    # ── 0 - 2 ans ────────────────────────────────────────────────────────────
    if re.search(r'\bstage\b|\bstagiaire\b|\bstagiar[e]?\b|\btirocinio\b|\bstage[ur]\b', t): return "0 - 2 ans"
    if re.search(r'\binterni?\b|\binternship\b|\bpraktikum\b|\bpraktikant\b', t): return "0 - 2 ans"
    if re.search(r'\balternanc[e]?\b|\balternant[e]?\b|\bapprentice\b|\bapprenti[e]?\b|\bausbildung\b', t): return "0 - 2 ans"
    if re.search(r'\btrainee\b|\btraining\s+contract\b|\bvie\b(?:\s|$)', t): return "0 - 2 ans"
    if re.search(r'\bjunior\b|\bjr\.?\b|\bdébutant[e]?\b|\bjeune\s+diplômé[e]?\b', t): return "0 - 2 ans"
    if re.search(r'\bworking\s+student\b|\bwerkstudent\b|\bwerkstudentin\b', t): return "0 - 2 ans"  # DE/EN étudiant

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
