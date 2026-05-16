#!/usr/bin/env python3
"""
Module partagé pour extraire le niveau d'études requis depuis le détail des offres.
Utilisé par les scrapers Oddo BHF, JP Morgan, Deloitte, Bpifrance, Société Générale, etc.

Formats de sortie harmonisés (compatibles offres.html) :
  "Bac"                  → Baccalauréat (secondaire)
  "Bac + 2 / L2"         → BTS, DUT, L2
  "Bac + 3 / L3"         → Bachelor, Licence, L3
  "Bac + 4 / M1"         → M1 (souvent regroupé avec Bac+5 par le frontend)
  "Bac + 5 / M2 et plus" → Master, MBA, Grande École, École d'ingénieur
  "Bac + 8 / Doctorat"   → PhD, Doctorat
"""

import re
from typing import Optional


def extract_education_level(
    text: str,
    contract_type: Optional[str] = None,
    job_title: Optional[str] = None,
) -> Optional[str]:
    """
    Extrait le niveau d'études depuis le texte de l'offre.
    Fallback : infère depuis le titre du poste (hiérarchie finance).

    Args:
        text:          Description complète de l'offre (short_desc, job_description…)
        contract_type: Type de contrat — VIE → Bac+5 en fallback
        job_title:     Titre du poste — fallback NLP quand la description ne donne rien

    Returns:
        Niveau d'études normalisé ou None si non déterminable.
    """
    text_lower = (text or "").lower().strip()

    # ─── 1. Patterns depuis la description ────────────────────────────────────
    if text_lower:
        # PhD / Doctorat (priorité absolue)
        if re.search(
            r"ph\.?d\.?|doctorat\b|doctorate\b|doctoral\b|"
            r"thèse\s+de\s+(?:doctorat|recherche)|\bthèse\b.*\brecherche\b",
            text_lower,
        ):
            return "Bac + 8 / Doctorat"

        # Bac+5 — Master / Grande École / Ingénieur
        if re.search(
            r"bac\s*[\+＋]\s*5"
            r"|master'?s?\b|mba\b|m\.?b\.?a\b"
            r"|m2\b|m\s*2\s*(?:minimum|requis|validé|en\s+cours)?"
            r"|grande\s+[eé]cole"
            r"|[eé]cole\s+d[e']\s*ing[eé]nieur"
            r"|[eé]cole\s+de\s+commerce"
            r"|diplôme\s+d[e']\s*ing[eé]nieur"
            r"|ing[eé]nieur\b"
            r"|engineering\s+school|business\s+school"
            r"|postgraduate\s+degree|graduate\s+degree|master'?s?\s+degree"
            r"|m\.?sc\.?\b|m\.?eng\.?\b"
            r"|\bmeister\b"                              # DE Meister ≈ expert
            r"|\bdiplom(?:\s+(?:informatik|wirtschaft|ingenieu|kaufman))?"  # DE Diplom (Uni)
            r"|\bgrand[e]?\s+[eé]cole\b"
            r"|niveau\s+bac\s*[+＋]\s*5",
            text_lower,
        ):
            return "Bac + 5 / M2 et plus"

        # Bac+4 — M1 (le frontend le groupe avec Bac+5)
        if re.search(
            r"bac\s*[\+＋]\s*4|m1\b|m\s*1\b|niveau\s+bac\s*[+＋]\s*4",
            text_lower,
        ):
            return "Bac + 4 / M1"

        # Bac+3 — Bachelor / Licence / BTS / DUT
        if re.search(
            r"bac\s*[\+＋]\s*3"
            r"|bachelor'?s?\b|bachelor'?s?\s+degree"
            r"|undergraduate\s+degree|undergraduate\s+student"
            r"|licence\b|licenciatura\b"
            r"|\bl3\b"
            r"|\bbs\b|\bba\b(?:\s+degree|\s+in\b|\s+or\b|$)"  # B.S. / B.A.
            r"|\bbsc\b|\bbachelor\s+of\b"
            r"|niveau\s+bac\s*[+＋]\s*3",
            text_lower,
        ):
            return "Bac + 3 / L3"

        # Bac+2 — BTS / DUT / Associate's degree
        if re.search(
            r"bac\s*[\+＋]\s*2"
            r"|\bbts\b|\bdut\b|\bl2\b"
            r"|associate'?s?\s+degree"
            r"|niveau\s+bac\s*[+＋]\s*2",
            text_lower,
        ):
            return "Bac + 2 / L2"

        # Bac — Secondaire
        if re.search(
            r"\bbac\b(?!\s*[\+＋])"
            r"|\blycée\b"
            r"|\bhigh\s+school\b|\bhighschool\b"
            r"|\babitur\b"                               # DE Abitur = Bac
            r"|\ba[\s-]levels?\b"                        # UK A-levels = Bac
            r"|\bcertificat\s+(?:fédéral|d[e']\s*capacité)\b"
            r"|niveau\s+bac(?!\s*[\+＋])",
            text_lower,
        ):
            return "Bac"

    # ─── 2. Règle contrat VIE ────────────────────────────────────────────────
    # Le VIE (Volontariat International) exige au minimum Bac+3 et attire
    # quasi-exclusivement des Bac+5 (réglementation Business France).
    if contract_type and "vie" in contract_type.lower():
        return "Bac + 5 / M2 et plus"

    # ─── 3. Inférence depuis le titre du poste ───────────────────────────────
    return _infer_education_from_title(job_title, contract_type)


def _infer_education_from_title(
    job_title: Optional[str],
    contract_type: Optional[str] = None,
) -> Optional[str]:
    """
    Infère le niveau d'études depuis le titre du poste (finance / conseil / IT).
    Seules les correspondances haute confiance sont utilisées.

    Hiérarchie :
      Bac+8  : PhD / Doctorat dans le titre
      Bac+5  : Rôles professionnels (Analyst, Associate, VP, Engineer, Consultant…)
      Bac+3  : Rôles opérationnels / support (Technicien, Agent, Intern non-master…)
      Bac    : Rarement inféré depuis le titre seul
    """
    if not job_title:
        return None
    t = job_title.lower()
    ct = (contract_type or "").lower()

    # ── Internship / Trainee → Bac+3 par défaut ──────────────────────────────
    # (Si la description contenait master/bac+5, c'était déjà traité ci-dessus)
    if re.search(r"\bstagiaire\b|\bpraktikant\w*\b|\bwerkstudent\w*\b", t):
        return "Bac + 3 / L3"
    if re.search(r"\bintern(?:ship)?\b|\btrainee\b", t):
        return "Bac + 3 / L3"
    # Alternance → Bac+3 sauf si le titre précise master/M2/ingénieur
    if "alternance" in ct or "apprenti" in ct or "ausbildung" in ct:
        if re.search(r"\bmaster\b|\bm2\b|\bing[eé]nieur\b|\bbac\s*\+?\s*5\b", t):
            return "Bac + 5 / M2 et plus"
        return "Bac + 3 / L3"

    # ── Bac+8 ────────────────────────────────────────────────────────────────
    if re.search(r"\bph\.?d\.?\b|\bdoctora[lt]\b|\bpostdoc\b", t):
        return "Bac + 8 / Doctorat"

    # ── Bac+5 — titres professionnels finance / conseil / IT ─────────────────
    # Managing Director / Executive Director / C-suite → toujours Bac+5
    if re.search(
        r"\bmanaging\s+director\b|\bexecutive\s+director\b"
        r"|\b(?:chief|ceo|cfo|cto|cio|cdo|coo|cro)\b"
        r"|\bvice\s+president\b|\bvp\b(?:\s|[-–]|$)",
        t,
    ):
        return "Bac + 5 / M2 et plus"

    # Director / Senior / Lead / Principal / Head
    if re.search(
        r"\bdirector\b|\bdirecteur\b|\bdirekteur\b|\bdirettore\b"
        r"|\bsenior\b|\bsr\.?\b"
        r"|\blead\b|\bleader\b|\bleitung\b"
        r"|\bprincipal\b"
        r"|\bhead\s+of\b|\bhead\b(?=\s)",
        t,
    ):
        return "Bac + 5 / M2 et plus"

    # Manager / Responsable
    if re.search(
        r"\bmanager\b|\bresponsable\b|\bleiter\b|\bgerente\b|\bjefe\b"
        r"|\bkierownik\b|\bkierowniczk\b",
        t,
    ):
        return "Bac + 5 / M2 et plus"

    # Analyst / Associate / Officer / Graduate — cœur finance IB
    if re.search(
        r"\banalyst[e]?\b|\banalytiker\b|\banalista\b|\banalityk\b"
        r"|\bassociat[e]?\b|\bassocié[e]?\b"
        r"|\bofficer\b",
        t,
    ):
        return "Bac + 5 / M2 et plus"

    # Graduate programme (≠ intern) → Bac+5
    if re.search(r"\bgraduate\s+programme?\b|\bgraduate\s+program\b", t):
        return "Bac + 5 / M2 et plus"

    # Specialist / Expert / Consultant / Advisor
    if re.search(
        r"\bspecialist[e]?\b|\bspécialiste\b|\bspezialist\b|\bspecjalista\b"
        r"|\bexpert[e]?\b|\bexperte?\b|\bexpérimenté[e]?\b"
        r"|\bconsultant[e]?\b|\bkonsultant\b|\bconsultor[a]?\b|\bberater\b"
        r"|\badvisor\b|\bconseiller\b|\bdoradca\b|\bdoradczyni\b",
        t,
    ):
        return "Bac + 5 / M2 et plus"

    # Engineer / Developer / Architect — IT et ingénierie
    if re.search(
        r"\bengineer\b|\bingénieur\b|\bingenieur\b|\bengineering\b"
        r"|\bdeveloper\b|\bdéveloppeur\b|\bentwickler\b|\bdesarrollador\b"
        r"|\barchitect[e]?\b|\barchitekten?\b"
        r"|\bdevops\b|\bdevsecops\b|\bsre\b",
        t,
    ):
        return "Bac + 5 / M2 et plus"

    # Finance / Audit / Risk / Compliance
    if re.search(
        r"\bauditor\b|\bauditeur\b|\bprüfer\b"
        r"|\bcontroller\b|\bcontrôleur\b|\bcontrolleur\b"
        r"|\btrader\b|\bquant\b|\bquantitative\b"
        r"|\bactuaire\b|\bactuary\b|\bactuarial\b"
        r"|\bjuriste\b|\battorney\b|\blawyer\b|\bparalegal\b"
        r"|\bscientist\b|\bscientifique\b|\bwissenschaftler\b"
        r"|\baccountant\b|\bcomptable\b",
        t,
    ):
        return "Bac + 5 / M2 et plus"

    # Chargé d'affaires / de mission / de projet / de conformité (sens professionnel en finance)
    # Gère les formes inclusives : Chargé.e, Chargé(e), CHARGE / CHARGEE
    # et les apostrophes droites/courbes + absence d'apostrophe ("D AFFAIRES")
    if re.search(
        r"\bcharg[eé]\.?\(?[e]?\)?\s*(?:/\s*charg[eé]e\s*)?"
        r"(?:d[e’‘'\s]\s*(?:affaires|mission|[eé]tudes?|conformit[eé]|projet|op[eé]rations?))"
        r"|\bchef\s+de\s+(?:projet|produit|mission)\b",
        t,
    ):
        return "Bac + 5 / M2 et plus"

    # MBA / Master dans le titre
    if re.search(r"\bmba\b|\bmaster\b|\bm\.?sc\b|\bm2\b", t):
        return "Bac + 5 / M2 et plus"

    # Banker / Banquier / Partner
    if re.search(r"\bbanker\b|\bbanquier\b|\bpartner\b|\bassocié[e]?\s+\w+", t):
        return "Bac + 5 / M2 et plus"

    # ── Bac+3 — rôles opérationnels / support ────────────────────────────────
    if re.search(
        r"\btechnicien\b|\btechnician\b|\btecnico\b|\btécnico\b"
        r"|\boperator\b|\bopérateur\b"
        r"|\bsuperviseur\b|\bsupervisor\b|\bsupervizor\b"
        r"|\binspect(?:eur|rice|or)\b",
        t,
    ):
        return "Bac + 3 / L3"

    # Agent d'accueil / commercial réseau (retail banking, souvent BTS)
    if re.search(
        r"\bagent\s+d[e']\s*(?:accueil|guichet|clientèle)\b"
        r"|\bconseiller\s+(?:clientèle|particuliers?|professionnels?)\s*(?:essentiel|prox|réseau)?\b"
        r"|\bchargé[e]?\s+d[e']\s*accueil\b",
        t,
    ):
        return "Bac + 3 / L3"

    # Réceptionniste, Caissier → Bac (rare dans notre corpus)
    if re.search(r"\bréceptionniste\b|\breceptionist\b|\bcaissier\b|\bcashier\b", t):
        return "Bac"

    return None


def normalize_education_level(raw: str) -> Optional[str]:
    """
    Normalise les valeurs issues des APIs (ex. Crédit Mutuel "BAC + 4 validé,
    BAC + 5 validé ou en cours") vers notre format standard.

    Retourne la valeur la plus haute mentionnée.
    """
    if not raw:
        return None
    r = raw.lower()

    if re.search(r"bac\s*[\+＋]\s*8|doctorat\b|ph\.?d", r):
        return "Bac + 8 / Doctorat"
    if re.search(r"bac\s*[\+＋]\s*5|master\b|mba\b|m2\b|ingénieur\b|grande.école|"
                 r"postgraduate|graduate\s+degree|m\.?sc\b", r):
        return "Bac + 5 / M2 et plus"
    if re.search(r"bac\s*[\+＋]\s*4|m1\b", r):
        return "Bac + 4 / M1"
    if re.search(r"bac\s*[\+＋]\s*3|bachelor\b|licence\b|l3\b|bsc\b", r):
        return "Bac + 3 / L3"
    if re.search(r"bac\s*[\+＋]\s*2|bts\b|dut\b|l2\b", r):
        return "Bac + 2 / L2"
    if re.search(r"\bbac\b(?!\s*[\+＋])|\bbac\s+valid[eé]\b|\blyc[eé]e\b", r):
        return "Bac"
    return None
