"""
Classifier les offres d'emploi par famille de métier
basé sur les titres et descriptions.

Hiérarchie deux niveaux :
  IT, Digital et Data
    ├─ Développement & Architecture
    ├─ Data & Intelligence Artificielle
    ├─ Cybersécurité
    ├─ Infrastructure & Cloud
    ├─ Gestion de Projet IT
    └─ Conseil & Solutions IT

  Commercial & Relations Clients
    ├─ Banque de Détail & Agence
    ├─ Banque Privée & Patrimoine
    └─ Banque Corporate & Marchés

  (Les autres familles restent au niveau 1)
"""
import re

# ─────────────────────────────────────────────────────────────────────────────
# SOUS-CATÉGORIES IT — ordre de priorité (du plus spécifique au plus générique)
# ─────────────────────────────────────────────────────────────────────────────
_IT_SUBCATEGORIES = [
    (
        "Cybersécurité",
        [
            r'\bcyber\b', r'\bcybersecurity\b', r'\bcybersécurité\b',
            r'\bsoc\b', r'\bsecurity\s+operat', r'\bpenetration\s+test', r'\bpentest\b',
            r'\bciso\b', r'\biam\b', r'\bidentity.*access\b', r'\bsiem\b',
            r'\bvulnerabilit', r'\bcryptograph', r'\bsécurité.*inform', r'\bsécurité.*réseau',
            r'\bsécurité.*données', r'\bsécurité.*cloud', r'\bsecops\b',
            r'\banti.*fraud\b', r'\bfraud.*detect', r'\bsécurité.*applicat',
        ]
    ),
    (
        "Data & Intelligence Artificielle",
        [
            r'\bdata\s+scien', r'\bdata\s+engineer', r'\bdata\s+analyst', r'\bdata\s+architect',
            r'\bdata\s+manag', r'\bdata\s+steward', r'\bdata\s+govern',
            r'\bmachine\s+learning\b', r'\bdeep\s+learning\b', r'\bartificial\s+intelligen',
            r'\bai\b', r'\bllm\b', r'\bmlops\b', r'\bnlp\b', r'\bcomputer\s+vision\b',
            r'\banalytics\b', r'\bbusiness\s+intelligence\b', r'\bBI\b',
            r'\bdataiku\b', r'\bsnowflake\b', r'\bdatabricks\b', r'\bspark\b',
            r'\bdatalake\b', r'\bdata\s+lake\b', r'\bdata\s+warehouse\b', r'\bdwh\b',
            r'\bpower\s*bi\b', r'\btableau\b', r'\bquantitative\b', r'\bquant\b',
            r'\bstatistique\b', r'\bmodélisation\b', r'\bmodel\s+risk\b',
        ]
    ),
    (
        "Infrastructure & Cloud",
        [
            r'\bcloud\b', r'\baws\b', r'\bazure\b', r'\bgcp\b', r'\bkubernetes\b', r'\bdocker\b',
            r'\binfrastructure\b', r'\bnetwork\b', r'\bréseau\b', r'\bsysadmin\b',
            r'\bsite\s+reliabilit', r'\bsre\b', r'\bdevsecops\b',
            r'\blinux\b', r'\bunix\b', r'\bvmware\b', r'\bvirtualisat', r'\bhypervisor\b',
            r'\bplatform\s+engineer', r'\bstorage\b', r'\bbackup\b', r'\bsaas\b',
            r'\bhpc\b', r'\btelecommunicat', r'\btélécom\b',
            r'\bsupport\s+(?:applicat|technique|it|n[°1-3])\b',
            r'\bmicroservices?\b', r'\borchestrat\b',
        ]
    ),
    (
        "Gestion de Projet IT",
        [
            r'\bproject\s+manager\b', r'\bprogram(?:me)?\s+manager\b',
            r'\bpmo\b', r'\bscrum\s+master\b', r'\bproduct\s+owner\b',
            r'\bagile\s+coach\b', r'\bdelivery\s+manager\b', r'\btransformat.*digit',
            r'\bgestion\s+de\s+projet\s+(?:it|informatiq|digital|tech)',
            r'\bprojet.*(?:si|systèmes?\s+d\'information|erp|sap|oracle)\b',
        ]
    ),
    (
        "Conseil & Solutions IT",
        [
            r'\bconsultant.*(?:it|tech|digital|sap|oracle|erp|crm|salesforce)\b',
            r'\b(?:sap|oracle|salesforce|servicenow|workday)\s+consultant\b',
            r'\barchitecte.*(?:solution|enterprise|applicat|données|si)\b',
            r'\bsolutions?\s+architect\b', r'\btechnical\s+(?:advisor|expert|specialist)\b',
            r'\bpré-?vente\b', r'\bpresales\b', r'\bintégrat.*(?:système|solution|si)\b',
            r'\bmaîtrise\s+d\'ouvrage\b', r'\bmaîtrise\s+d\'oeuvre\b',
            r'\bmoa\b', r'\bmoe\b', r'\bamoa\b',
            r'\bconseil.*digital\b', r'\bdigital.*transformat\b',
        ]
    ),
    (
        "Développement & Architecture",
        [
            r'\bdeveloper\b', r'\bdéveloppeur\b', r'\bdeveloppeur\b', r'\bsoftware\s+engineer\b',
            r'\bfull\s*stack\b', r'\bfront.?end\b', r'\bback.?end\b',
            r'\bjava\b', r'\bpython\b', r'\.net\b', r'\bc\+\+\b', r'\btypescript\b',
            r'\breact\b', r'\bangular\b', r'\bvue\.?js\b', r'\bnode\.?js\b',
            r'\bkotlin\b', r'\bswift\b', r'\bphp\b', r'\bruby\b', r'\bscala\b',
            r'\bmicroservice\b', r'\bapi\b', r'\brest\s+api\b', r'\bgraphql\b',
            r'\barchitect(?:e|ure)?\s+(?:logiciel|applicat|tech|si)\b',
            r'\barchitecte\s+technique\b', r'\btechnical\s+architect\b',
            r'\bqa\b', r'\bquality\s+assurance\b', r'\btest\s+(?:engineer|automation|lead)\b',
            r'\bautomation\s+engineer\b', r'\bprogramm(?:eur|ing)\b', r'\bcod(?:eur|ing)\b',
        ]
    ),
]

# ─────────────────────────────────────────────────────────────────────────────
# SOUS-CATÉGORIES COMMERCIAL
# ─────────────────────────────────────────────────────────────────────────────
_COMMERCIAL_SUBCATEGORIES = [
    (
        "Banque Privée & Patrimoine",
        [
            r'\bprivate\s+bank', r'\bwealth\s+manag', r'\bgestion\s+de\s+(?:fortune|patrimoine)\b',
            r'\bpatrimoine\b', r'\bconseiller.*privé\b', r'\bbanquier.*privé\b',
            r'\bhigh\s+net\s+worth\b', r'\bhnw\b', r'\buhnw\b', r'\bvip\b',
            r'\bgestion.*actifs.*particulier', r'\bfamily\s+office\b',
        ]
    ),
    (
        "Banque Corporate & Marchés",
        [
            r'\bcorporate\s+(?:bank|client|finance|cover)\b',
            r'\bcoverage\b', r'\brelationship\s+(?:manager|banker)\b',
            r'\bcapital\s+markets?\b', r'\btrade\s+finance\b',
            r'\bfixed\s+income\b', r'\bequit(?:y|ies)\s+(?:sales|research)\b',
            r'\bsales\s+(?:trader|capital|fixed|equity)\b',
            r'\bgrand(?:e)?s?\s+entreprises?\b', r'\bclient(?:s)?\s+corporate\b',
            r'\bfinancement\s+(?:corporate|entreprise|structure)\b',
            r'\btrésorerie\s+d\'entreprise\b', r'\bcash\s+management.*entreprise\b',
            r'\bpme\s+(?:et|\/)\s*eti\b', r'\bcompte\s+clé\b', r'\bkey\s+account\b',
        ]
    ),
    (
        "Banque de Détail & Agence",
        [
            r'\bconseiller.*(?:particulier|clientèle|essentiel|premium|professionnel)\b',
            r'\bchargé.*(?:clientèle|client)\b', r'\bagent.*(?:commercial|général)\b',
            r'\bdirecteur.*agence\b', r'\bresponsable.*agence\b', r'\badjoint.*agence\b',
            r'\baccueil.*client\b', r'\bguichet\b', r'\btelleuse?\b',
            r'\bconseiller.*pro\b', r'\bbanque.*détail\b', r'\bretail\s+bank',
            r'\bmandataire\b', r'\bcourtier\b', r'\bassurance.*conseil\b',
            r'\bconseiller.*assurance\b',
        ]
    ),
]

# ─────────────────────────────────────────────────────────────────────────────
# FAMILLES NIVEAU 1 (ordre de priorité : le premier match gagne)
# ─────────────────────────────────────────────────────────────────────────────
JOB_FAMILIES = {
    # ── IT — les sous-catégories sont gérées en aval, ici c'est le filet ──────
    "IT, Digital et Data": [
        r'\bdata\b', r'\bIT\b', r'\bdigital\b', r'\bengineer\b', r'\bdeveloper\b',
        r'\bdevops\b', r'\bsoftware\b', r'\bprogramm', r'\bcybers?ecurity\b', r'\bcyber\b', r'\bcloud\b',
        r'\binfra', r'\bsystem\b', r'\bnetwork\b', r'\bjava\b', r'\bpython\b',
        r'\b\.net\b', r'\bfullstack\b', r'\bfull stack\b', r'\bbackend\b',
        r'\bfrontend\b', r'\bfront-end\b', r'\bback-end\b', r'\bsql\b',
        r'\bdatabase\b', r'\bteradata\b', r'\bpostgres\b', r'\bmongodb\b',
        r'\boracle\b', r'\barchitecte technique\b', r'\bscrum\b', r'\bagile\b',
        r'\btechnical lead\b', r'\btech lead\b', r'\bsite reliability\b',
        r'\bmachine learning\b', r'\bartificial intelligence\b', r'\bIA\b',
        r'\bdata scien', r'\bdata engineer\b', r'\bdata analyst\b', r'\bbigdata\b',
        r'\banalyst.*data\b', r'\bBI\b', r'\bbusiness intelligence\b',
        r'\bapplicat(if|ion)\b', r'\bsupport.*applicat\b', r'\bQA\b',
        r'\btest', r'\bqualité.*logiciel\b', r'\binformatique\b', r'\banalytics\b',
        r'\bsap\b', r'\bsalesforce\b', r'\bservicenow\b', r'\bworkday\b',
    ],

    "Commercial / Relations Clients": [
        r'\bconseiller.*clientèle\b', r'\bconseiller.*client\b', r'\bchargé.*clientèle\b',
        r'\brelation.*client\b', r'\bclient.*relation\b', r'\bcommercial\b',
        r'\bvente\b', r'\bsales\b', r'\bbanquier\b', r'\bgestionnaire.*patrimoine\b',
        r'\bpatrimonial\b', r'\bagent.*commercial\b', r'\bdirecteur.*agence\b',
        r'\bresponsable.*agence\b', r'\badjoint.*agence\b', r'\bagence\b',
        r'\bconseiller.*particulier\b', r'\bconseiller.*professionnel\b',
        r'\bconseiller.*essentiel\b', r'\bconseiller.*premium\b', r'\bconseiller.*privé\b',
        r'\bprivate bank', r'\bcoverage\b', r'\brelationship manager\b',
        r'\baccount manager\b', r'\bclient.*advisor\b', r'\bcustomer.*advisor\b',
        r'\bmandataire\b', r'\bcourtier\b',
    ],

    "Financement et Investissement": [
        r'\bfinance\b', r'\bfinancing\b', r'\binvestment\b', r'\binvestissement\b',
        r'\bM&A\b', r'\bfusions.*acquisitions\b', r'\bcorporate.*finance\b',
        r'\bproject.*finance\b', r'\bstructur.*finance\b', r'\btrade.*finance\b',
        r'\bcrédit\b', r'\bcredit\b', r'\bprêt\b', r'\bloan\b', r'\bleasing\b',
        r'\bfactoring\b', r'\banalyste.*crédit\b', r'\bcredit.*analyst\b',
        r'\bchargé.*crédit\b', r'\bcredit.*officer\b', r'\bfinancement\b',
        r'\bequity\b', r'\bdebt\b', r'\bcapital.*markets\b', r'\bmarkets\b',
        r'\btrading\b', r'\btrader\b', r'\bquant\b', r'\bstructuration\b',
        r'\bproduit.*financier\b', r'\bfinancial.*product\b',
    ],

    "Risques / Contrôles permanents": [
        r'\brisque\b', r'\brisk\b', r'\bERM\b', r'\bcontrôle.*risque\b',
        r'\brisk.*control\b', r'\brisk.*manage', r'\bmodel.*risk\b',
        r'\bcredit.*risk\b', r'\bmarket.*risk\b', r'\boperational.*risk\b',
        r'\brisque.*opérationnel\b', r'\brisque.*crédit\b', r'\brisque.*marché\b',
        r'\bcontrôle.*permanent\b', r'\bpermanent.*control\b', r'\binternal.*control\b',
        r'\bcontrôle.*interne\b', r'\bvalidation.*modèle\b', r'\bmodel.*validation\b',
    ],

    "Conformité / Sécurité financière": [
        r'\bconformité\b', r'\bcompliance\b', r'\bKYC\b', r'\bAML\b', r'\bAMLO\b',
        r'\banti.*money.*launder\b', r'\banti.*blanch', r'\bLCB-FT\b',
        r'\bsécurité.*financière\b', r'\bfinancial.*security\b', r'\bfraud\b',
        r'\bfraude\b', r'\bréglement', r'\bregulat', r'\bréglementaire\b',
    ],

    "Finances / Comptabilité / Contrôle de gestion": [
        r'\bcomptab', r'\baccounting\b', r'\bcomptable\b', r'\baccountant\b',
        r'\bcontrôle.*gestion\b', r'\bmanagement.*control\b', r'\bcontrol.*gestion\b',
        r'\bfinancial.*control\b', r'\bcontrôleur.*gestion\b', r'\bcontroller\b',
        r'\bbudget\b', r'\bconsolidation\b', r'\breporting.*financier\b',
        r'\bfinancial.*reporting\b', r'\bFP&A\b', r'\btrésor', r'\btreasur',
        r'\bcash.*management\b', r'\bback.*office.*comptab\b',
    ],

    "Gestion des opérations": [
        r'\bopérations\b', r'\boperations\b', r'\bback.*office\b', r'\bmiddle.*office\b',
        r'\bpost.*trade\b', r'\bsettlement\b', r'\bclearing\b', r'\bcustody\b',
        r'\breconciliation\b', r'\brapprochement\b', r'\bprocessing\b',
        r'\btraitement.*opération\b', r'\bgestionnaire.*opération\b',
        r'\boperation.*manager\b', r'\bprocess.*manager\b',
    ],

    "Ressources Humaines": [
        r'\bRH\b', r'\bHR\b', r'\bhuman.*resource\b', r'\bressources?\s+humaines?\b',
        r'\brecrutement\b', r'\brecruitment\b', r'\btalent\b', r'\bformation\b',
        r'\btraining\b', r'\bpaye\b', r'\bpayroll\b', r'\bcompensation\b',
        r'\brémunération\b', r'\bpeople\b', r'\bemployee\b', r'\bsalarié\b',
    ],

    "Juridique": [
        r'\bjuridique\b', r'\blegal\b', r'\bavocat\b', r'\blawyer\b',
        r'\bconseiller.*juridique\b', r'\blegal.*counsel\b', r'\bcontrat\b',
        r'\bcontract\b', r'\bdroit\b', r'\blaw\b', r'\blitigation\b',
        r'\bcontentieux\b',
    ],

    "Marketing et Communication": [
        r'\bmarketing\b', r'\bcommunication\b', r'\bpublicité\b', r'\badvertising\b',
        r'\bbrand\b', r'\bmarque\b', r'\bdigital.*marketing\b', r'\bcontent\b',
        r'\bsocial.*media\b', r'\bréseaux.*sociaux\b', r'\bevent\b', r'\bévénement\b',
    ],

    "Inspection / Audit": [
        r'\baudit\b', r'\binspection\b', r'\binspecteur\b', r'\bauditor\b',
        r'\binternal.*audit\b', r'\baudit.*interne\b', r'\bcontrôle.*qualité\b',
    ],

    "Analyse financière et économique": [
        r'\banalyste.*financier\b', r'\bfinancial.*analyst\b', r'\béconomiste\b',
        r'\beconomist\b', r'\banalyst.*economic\b', r'\banalyse.*économique\b',
        r'\bsearch\b', r'\bétude.*économique\b',
    ],

    "Organisation / Qualité": [
        r'\borganisation\b', r'\bqualité\b', r'\bquality\b', r'\bprocess\b',
        r'\bamélioration.*continue\b', r'\bcontinuous.*improvement\b',
        r'\blean\b', r'\bsix.*sigma\b', r'\btransformation\b',
        r'\bchef.*projet\b', r'\bproject.*management\b',
    ],

    "Achat": [
        r'\bachat\b', r'\bpurchas', r'\bprocurement\b', r'\bacheteur\b',
        r'\bbuyer\b', r'\bsourcing\b', r'\bfournisseur\b', r'\bsupplier\b',
    ],
}


def _score_subcat(text: str, title: str, patterns: list) -> int:
    """Score un texte contre une liste de patterns regex."""
    score = 0
    for p in patterns:
        if re.search(p, title, re.IGNORECASE):
            score += 3
        elif re.search(p, text, re.IGNORECASE):
            score += 1
    return score


def _classify_it_subcat(job_title: str, job_description: str = "") -> str:
    """Retourne la sous-catégorie IT la plus appropriée."""
    text = (job_description or "")[:3000]
    title = job_title or ""
    best_cat, best_score = "IT, Digital et Data", 0
    for cat, patterns in _IT_SUBCATEGORIES:
        s = _score_subcat(text, title, patterns)
        if s > best_score:
            best_score, best_cat = s, cat
    return best_cat


def _classify_commercial_subcat(job_title: str, job_description: str = "") -> str:
    """Retourne la sous-catégorie Commercial la plus appropriée."""
    text = (job_description or "")[:3000]
    title = job_title or ""
    best_cat, best_score = "Commercial / Relations Clients", 0
    for cat, patterns in _COMMERCIAL_SUBCATEGORIES:
        s = _score_subcat(text, title, patterns)
        if s > best_score:
            best_score, best_cat = s, cat
    return best_cat


def classify_job_family(job_title: str, job_description: str = "") -> str:
    """
    Classifie une offre dans une famille de métier.
    Pour IT et Commercial, retourne directement la sous-catégorie.

    Args:
        job_title: Titre du poste
        job_description: Description du poste (optionnel)

    Returns:
        Nom de la famille (ou sous-famille) de métier, ou "Autres"
    """
    text = f"{job_title} {job_description}".lower()
    title_lower = (job_title or "").lower()

    family_scores = {}
    for family, patterns in JOB_FAMILIES.items():
        score = 0
        for pattern in patterns:
            if re.search(pattern, text, re.IGNORECASE):
                score += 3 if re.search(pattern, title_lower, re.IGNORECASE) else 1
        family_scores[family] = score

    if family_scores:
        best_family, best_score = max(family_scores.items(), key=lambda x: x[1])
        if best_score > 0:
            # Affiner en sous-catégorie pour IT et Commercial
            if best_family == "IT, Digital et Data":
                return _classify_it_subcat(job_title, job_description)
            if best_family == "Commercial / Relations Clients":
                return _classify_commercial_subcat(job_title, job_description)
            return best_family

    return "Autres"
