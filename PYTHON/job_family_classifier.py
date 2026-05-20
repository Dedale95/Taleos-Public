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
            r'\bcybers?ecurity\b', r'\bcybersécurité\b', r'\bcyber\b',
            r'\bsoc\b', r'\bsecurity\s+operat', r'\bpenetration\s+test', r'\bpentest\b',
            r'\bciso\b', r'\biam\b', r'\bidentity.*access\b', r'\bsiem\b',
            r'\bvulnerabilit', r'\bcryptograph', r'\bsécurité.*inform', r'\bsécurité.*réseau',
            r'\bsécurité.*données', r'\bsécurité.*cloud', r'\bsecops\b',
            r'\banti.*fraud\b', r'\bfraud.*detect', r'\bsécurité.*applicat',
            r'\bforensic\b', r'\bthreat.*intel', r'\bincident.*response\b',
            r'\bsecurity\s+engineer', r'\bsecurity\s+(?:architect|lead|director|manager|controls?|analyst)\b',
            r'\blead\s+security\b', r'\bprincipal\s+security\b',
            r'\boffensive.*security\b', r'\bdefensive.*security\b', r'\bblockchain.*security\b',
        ]
    ),
    (
        "Data & Intelligence Artificielle",
        [
            r'\bdata\s+scien', r'\bdata\s+engineer', r'\bdata\s+anal', r'\bdata\s+architect',
            r'\bdata\s+manag', r'\bdata\s+steward', r'\bdata\s+govern', r'\bdata\s+qualit',
            r'\bdata\s+owner\b', r'\bdata\s+operat', r'\bdata\s+domain\b', r'\bdata\s+product\b',
            r'\barchitecte.*data\b', r'\bingénieur.*données\b',
            r'\baiml\b', r'\bapplied\s+(?:ai|ml|aiml)\b',
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
            # Cloud & plateformes
            r'\bcloud\b', r'\baws\b', r'\bazure\b', r'\bgcp\b', r'\boci\b',
            r'\bkubernetes\b', r'\bdocker\b', r'\bterraform\b', r'\bansible\b',
            r'\bci[/-]?cd\b', r'\bpipeline\b', r'\bjenkins\b',
            # Infrastructure & réseau
            r'\binfrastructure\b', r'\bnetwork\b', r'\bréseau\b', r'\bsysadmin\b',
            r'\bsite\s+reliabilit', r'\bsre\b', r'\bdevsecops\b', r'\bdevops\b',
            r'\blinux\b', r'\bunix\b', r'\bvmware\b', r'\bvirtualisat', r'\bhypervisor\b',
            r'\bplatform\s+engineer', r'\bstorage\b', r'\bbackup\b', r'\bsaas\b',
            r'\bhpc\b', r'\btelecommunicat', r'\btélécom\b',
            # Support & exploitation
            r'\bsupport\s+(?:applicat|technique|it|n[°1-3])\b',
            r'\bapplication\s+support\b', r'\bproduction\s+support\b',
            r'\bexploitation\b', r'\brun\s+(?:it|applicat)\b',
            r'\bmonitoring\b', r'\bobservabilit', r'\bincident.*manag',
            r'\bservice\s+desk\b', r'\bhelpdesk\b',
            r'\btechnology\s+support\b', r'\btech(?:nology)?\s+(?:support|operat|infra)\b',
            r'\bplatform.*operat', r'\bit\s+operat',
            # Asset management
            r'\bitam\b', r'\bit\s+asset',
            r'\bmicroservices?\b', r'\borchestrat\b',
        ]
    ),
    (
        "Gestion de Projet IT",
        [
            r'\bproject\s+manager\b', r'\bprogram(?:me)?\s+manager\b',
            r'\bpmo\b', r'\bscrum\s+master\b', r'\bproduct\s+owner\b',
            r'\bagile\s+coach\b', r'\bdelivery\s+manager\b',
            r'\bgestion\s+de\s+projet\b',  # chef de projet sans qualifier → OK dans contexte IT
            r'\bchef\s+de\s+projet\b',
            r'\btransformat.*digit', r'\bdigital.*transformat',
            r'\bgestion\s+de\s+projet\s+(?:it|informatiq|digital|tech)',
            r'\bprojet.*(?:si|systèmes?\s+d\'information|erp|sap|oracle|digital)\b',
            r'\bit\s+(?:project|program|governance)\b',
            r'\bgouvernance.*(?:it|si|tech|digital)\b',
            r'\brisques?\s+(?:digit|si\b|systèmes?\s+d\'information)\b',
        ]
    ),
    (
        "Conseil & Solutions IT",
        [
            # Business Analysis
            r'\bbusiness\s+anal',
            r'\bfront\s*[-\s]?office\s+(?:it|analyst|tech)\b',
            # Conseil IT & Digital
            r'\bconsultant.*(?:it|tech|digital|sap|oracle|erp|crm|salesforce|strateg|transformat)\b',
            r'\bdigital.*consult', r'\bconseil.*(?:it|digital|si|tech)\b',
            r'\bit\s+strateg', r'\bcio\b', r'\bcto\b',
            # Solutions & Architectures
            r'\b(?:sap|oracle|salesforce|servicenow|workday|dynamics)\s+(?:consultant|architect|advisor)\b',
            r'\bdynamics\s*365\b', r'\bdynamics\b',
            r'\barchitecte.*(?:solution|enterprise|applicat|données|si)\b',
            r'\bsolutions?\s+architect\b',
            r'\btechnical\s+(?:advisor|expert|specialist|lead)\b',
            r'\btech\s+lead\b',
            r'\bexpert.*(?:it|tech|digital|si)\b',
            # Pré-vente & intégration
            r'\bpré-?vente\b', r'\bpresales\b', r'\bintégrat.*(?:système|solution|si)\b',
            r'\bmaîtrise\s+d\'ouvrage\b', r'\bmaîtrise\s+d\'oeuvre\b',
            r'\bmoa\b', r'\bmoe\b', r'\bamoa\b',
        ]
    ),
    (
        "Développement & Architecture",
        [
            # Développeurs
            r'\bdeveloper\b', r'\bdéveloppeur\b', r'\bdeveloppeur\b', r'\bsoftware\s+engineer\b',
            r'\bfull\s*stack\b', r'\bfront.?end\b', r'\bback.?end\b',
            # Langages
            r'\bjava\b', r'\bpython\b', r'\.net\b', r'\bc\+\+\b', r'\btypescript\b',
            r'\breact\b', r'\bangular\b', r'\bvue\.?js\b', r'\bnode\.?js\b',
            r'\bkotlin\b', r'\bswift\b', r'\bphp\b', r'\bruby\b', r'\bscala\b',
            r'\bcobol\b', r'\bpl[\s/]?sql\b',
            # API & architecture logicielle
            r'\bmicroservice\b', r'\bapi\b', r'\brest\s+api\b', r'\bgraphql\b',
            r'\barchitect(?:e|ure)?\s+(?:logiciel|applicat|tech|si)\b',
            r'\barchitecte\s+technique\b', r'\btechnical\s+architect\b',
            # Bases de données
            r'\bdba\b', r'\bdatabase\s+admin', r'\bpostgres\b', r'\bmysql\b',
            r'\bmongodb\b', r'\bteradata\b', r'\bsql\b',
            # QA & tests
            r'\bqa\b', r'\bquality\s+assurance\b', r'\btest\s+(?:engineer|automation|lead)\b',
            r'\bautomation\s+engineer\b', r'\bprogramm(?:eur|ing)\b', r'\bcod(?:eur|ing)\b',
            r'\buat\b', r'\bsoftware\s+engineer',  # sans \b final pour matcher "engineering"
            r'\bprincipal\s+architect\b', r'\blead\s+(?:architect|developer|engineer)\b',
            r'\bengineer(?:ing)?\s+manager\b', r'\bmanager\s+of\s+software\b',
            r'\bsr\.?\s+(?:engineer|developer|architect)\b',
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
            # Anglais
            r'\bprivate\s+bank', r'\bwealth\s+manag', r'\bhigh\s+net\s+worth\b',
            r'\bhnw\b', r'\buhnw\b', r'\bfamily\s+office\b',
            r'\bwealth\s+advisor\b', r'\bprivate\s+advisor\b',
            # Français (avec variantes accentuées)
            r'\bgestion\s+de\s+(?:fortune|patrimoine)\b',
            r'\bpatrimoine\b', r'\bpatrimonial',
            r'\bgestion\s+priv[eé]\b', r'\bgestion\s+de\s+patrimoine\b',
            r'\bconseiller.*(?:priv[eé]|patrimoni|fortune)\b',
            r'\bbanquier.*priv[eé]',
            r'\bcharg[eé].*affaires.*(?:patrimoin|priv[eé]|gestion\s+priv)',
            r'\bgestionnaire.*clientèle.*patrimoni',
            r'\bgestionnaire.*patrimoni',
            r'\bgestion.*actifs.*particulier',
        ]
    ),
    (
        "Banque Corporate & Marchés",
        [
            # Corporate banking
            r'\bcorporate\s+(?:bank|client|finance|cover)\b',
            r'\bcoverage\b', r'\brelationship\s+(?:manager|banker)\b',
            r'\bgrand(?:e)?s?\s+entreprises?\b', r'\bclient(?:s)?\s+corporate\b',
            r'\bfinancement\s+(?:corporate|entreprise|structure)\b',
            r'\bbanquier.*entreprise\b',
            r'\bcharg[eé].*affaires.*(?:entreprises?|corporate|pme|eti|personnes?\s+moral)',
            r'\bdirecteur.*(?:entreprise|corporate|comptes)\b',
            r'\bpme\s+(?:et|\/)\s*eti\b', r'\bkey\s+account\b', r'\bcompte\s+clé\b',
            # Marchés & produits
            r'\bcapital\s+markets?\b', r'\btrade\s+finance\b',
            r'\bfixed\s+income\b', r'\bequit(?:y|ies)\s+(?:sales|research)\b',
            r'\bsales\s+(?:trader|capital|fixed|equity)\b',
            r'\btrésorerie\s+d\'entreprise\b', r'\bcash\s+management.*entreprise\b',
        ]
    ),
    (
        "Banque de Détail & Agence",
        [
            # Conseillers
            r'\bconseiller.*(?:particulier|clientèle|essentiel|premium|professionnel|financier)\b',
            r'\bconseiller\s+commercial\b', r'\bassistant\s+commercial\b',
            r'\bconseiller.*(?:pro\b|professionnel)\b',
            r'\bconseiller.*assurance\b',
            # Chargés de clientèle
            r'\bcharg[eé].*(?:clientèle|client)\b',
            r'\bcharg[eé].*affaires.*(?:particulier|essentiel)\b',
            # Gestionnaires clientèle (très commun chez BP, CA, CM, etc.)
            r'\bgestionnaire.*client',
            r'\bgestionnaire.*(?:clientèle|comptes)\b',
            r'\bdirecteur.*(?:client|clientèle)\b',
            # Agences & réseaux
            r'\bagent.*(?:commercial|général)\b',
            r'\bdirecteur.*agence\b', r'\bresponsable.*agence\b', r'\badjoint.*agence\b',
            r'\baccueil.*client\b', r'\bguichet\b', r'\btelleuse?\b',
            r'\bbanque.*d[eé]tail\b', r'\bretail\s+bank',
            r'\bresponsable.*point\s+de\s+vente\b', r'\bresponsable.*pdv\b',
            # Bankers & associates (JP Morgan style)
            r'\bassociate\s+banker\b', r'\bprivate\s+client\s+banker\b',
            r'\bclient\s+associate\b', r'\bclient\s+service\b',
            r'\bbanque\s+à\s+distance\b', r'\bteleconseil\b', r'\bteleconseill',
            # Mandataires & courtiers
            r'\bmandataire\b', r'\bcourtier\b',
            r'\bassurance.*conseil\b',
            # Développement commercial général
            r'\bdéveloppement\s+commercial\b', r'\bportefeuille.*client\b',
            r'\bcharg[eé].*accueil\b', r'\bsatisfaction.*client\b',
            r'\breclamation.*client\b', r'\brelation.*client\b',
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
        r'\baudit(?:eur|or|ing)?\b', r'\baudit\b',  # "auditeur" ne matchait pas \baudit\b
        r'\binspection\b', r'\binspecteur\b',
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
    """Retourne la sous-catégorie IT la plus appropriée.
    Retourne 'IT - Autres' si aucun pattern ne matche (fallback distinct du parent)."""
    text = (job_description or "")[:3000]
    title = job_title or ""
    best_cat, best_score = "IT - Autres", 0
    for cat, patterns in _IT_SUBCATEGORIES:
        s = _score_subcat(text, title, patterns)
        if s > best_score:
            best_score, best_cat = s, cat
    return best_cat


def _classify_commercial_subcat(job_title: str, job_description: str = "") -> str:
    """Retourne la sous-catégorie Commercial la plus appropriée.
    Retourne 'Commercial - Autres' si aucun pattern ne matche."""
    text = (job_description or "")[:3000]
    title = job_title or ""
    best_cat, best_score = "Commercial - Autres", 0
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
