"""
Normalisation des noms de pays
EN → FR pour cohérence
"""

# Villes connues : ville (lowercase) → pays en français
# Utilisé pour corriger les offres (ODDO BHF, Deloitte, etc.) où le pays manque ou est erroné
CITY_TO_COUNTRY = {
    # France (villes courantes pour ODDO BHF et autres sources sans pays)
    'paris': 'France',
    'lyon': 'France',
    'clichy': 'France',
    'marseille': 'France',
    'toulouse': 'France',
    'bordeaux': 'France',
    'lille': 'France',
    'nice': 'France',
    'nantes': 'France',
    'strasbourg': 'France',
    'rennes': 'France',
    'montpellier': 'France',
    'saint-denis': 'France',
    'reims': 'France',
    'le havre': 'France',
    'saint-étienne': 'France',
    'toulon': 'France',
    'grenoble': 'France',
    'dijon': 'France',
    'angers': 'France',
    'nîmes': 'France',
    'villeurbanne': 'France',
    'aix-en-provence': 'France',
    'nanterre': 'France',
    'courbevoie': 'France',
    'levallois-perret': 'France',
    'neuilly-sur-seine': 'France',
    'puteaux': 'France',
    'la défense': 'France',
    'suresnes': 'France',
    # Tunisie
    'tunis': 'Tunisie',
    # Afrique subsaharienne
    'douala': 'Cameroun',
    'yaoundé': 'Cameroun',
    'yaounde': 'Cameroun',
    'lubumbashi': 'République Démocratique du Congo',
    'kinshasa': 'République Démocratique du Congo',
    'libreville': 'Gabon',
    'port-gentil': 'Gabon',
    'cotonou': 'Bénin',
    'abidjan': "Côte d'Ivoire",
    'dakar': 'Sénégal',
    'bamako': 'Mali',
    'ouagadougou': 'Burkina Faso',
    'niamey': 'Niger',
    'lomé': 'Togo',
    'lome': 'Togo',
    'accra': 'Ghana',
    'lagos': 'Nigeria',
    'abuja': 'Nigeria',
    'casablanca': 'Maroc',
    'rabat': 'Maroc',
    'marrakech': 'Maroc',
    'alger': 'Algérie',
    'oran': 'Algérie',
    'cairo': 'Égypte',
    'le caire': 'Égypte',
    'johannesburg': 'Afrique du Sud',
    'le cap': 'Afrique du Sud',
    'cape town': 'Afrique du Sud',
    # Autres villes internationales courantes
    'londres': 'Royaume-Uni',
    'london': 'Royaume-Uni',
    'dublin': 'Irlande',
    'amsterdam': 'Pays-Bas',
    'bruxelles': 'Belgique',
    'brussels': 'Belgique',
    'brussel': 'Belgique',
    'luxembourg': 'Luxembourg',
    'saint-pierre-port': 'Guernesey',
    'genève': 'Suisse',
    'geneve': 'Suisse',
    'zurich': 'Suisse',
    'madrid': 'Espagne',
    'barcelone': 'Espagne',
    'barcelona': 'Espagne',
    'rome': 'Italie',
    'roma': 'Italie',
    'milan': 'Italie',
    'milano': 'Italie',
    'berlin': 'Allemagne',
    'munich': 'Allemagne',
    'francfort': 'Allemagne',
    'frankfurt': 'Allemagne',
    'vienne': 'Autriche',
    'vienna': 'Autriche',
    'lisbonne': 'Portugal',
    'lisbon': 'Portugal',
    'varsovie': 'Pologne',
    'warsaw': 'Pologne',
    'prague': 'République Tchèque',
    'budapest': 'Hongrie',
    'bucarest': 'Roumanie',
    'bucharest': 'Roumanie',
    'moscou': 'Russie',
    'moscow': 'Russie',
    'istanbul': 'Turquie',
    'athènes': 'Grèce',
    'athens': 'Grèce',
    'new york': 'États-Unis',
    'los angeles': 'États-Unis',
    'chicago': 'États-Unis',
    'san francisco': 'États-Unis',
    'montréal': 'Canada',
    'montreal': 'Canada',
    'toronto': 'Canada',
    'quebec': 'Canada',
    'québec': 'Canada',
    'vancouver': 'Canada',
    'singapour': 'Singapour',
    'singapore': 'Singapour',
    'hong kong': 'Hong-Kong',
    'hong-kong': 'Hong-Kong',
    'tokyo': 'Japon',
    'jakarta': 'Indonésie',
    'djibouti': 'Djibouti',
    'sydney': 'Australie',
    'melbourne': 'Australie',
    'dubaï': 'Émirats Arabes Unis',
    'dubai': 'Émirats Arabes Unis',
    'abu dhabi': 'Émirats Arabes Unis',
    'bombay': 'Inde',
    'mumbai': 'Inde',
    'bangalore': 'Inde',
    'delhi': 'Inde',
    'chennai': 'Inde',
    'shanghai': 'Chine',
    'pékin': 'Chine',
    'pekin': 'Chine',
    'beijing': 'Chine',
    'séoul': 'Corée du Sud',
    'seoul': 'Corée du Sud',
    'tel aviv': 'Israël',
    'riyadh': 'Arabie Saoudite',
    'doha': 'Qatar',
    'nassau': 'Bahamas',
    'panama city': 'Panama',
    'panama': 'Panama',
    'kuwait city': 'Koweït',
    'kuwait': 'Koweït',
    'lima': 'Pérou',
    'kuala lumpur': 'Malaisie',
    'bangkok': 'Thaïlande',
    'hanoï': 'Vietnam',
    'hanoi': 'Vietnam',
    'ho chi minh': 'Vietnam',
    'saigon': 'Vietnam',
}


def get_country_from_city(city):
    """
    Retourne le pays pour une ville connue (France, Tunisie, etc.).
    Retourne None si la ville est inconnue.
    """
    if not city or not isinstance(city, str):
        return None
    key = city.strip().lower()
    return CITY_TO_COUNTRY.get(key)


COUNTRY_MAPPING = {
    # Anglais → Français
    'united states': 'États-Unis',
    'united states of america': 'États-Unis',
    'usa': 'États-Unis',
    'united kingdom': 'Royaume-Uni',
    'uk': 'Royaume-Uni',
    'germany': 'Allemagne',
    'spain': 'Espagne',
    'italy': 'Italie',
    'netherlands': 'Pays-Bas',
    'belgium': 'Belgique',
    'switzerland': 'Suisse',
    'austria': 'Autriche',
    'poland': 'Pologne',
    'czech republic': 'Tchéquie',
    'czechia': 'Tchéquie',
    'romania': 'Roumanie',
    'hungary': 'Hongrie',
    'portugal': 'Portugal',
    'greece': 'Grèce',
    'denmark': 'Danemark',
    'sweden': 'Suède',
    'norway': 'Norvège',
    'finland': 'Finlande',
    'ireland': 'Irlande',
    'russia': 'Russie',
    'turkey': 'Turquie',
    
    # Asie / Océanie
    'india': 'Inde',
    'china': 'Chine',
    'japan': 'Japon',
    'south korea': 'Corée du Sud',
    'korea': 'Corée du Sud',
    'taiwan': 'Taïwan',
    'taïwan': 'Taïwan',
    'singapore': 'Singapour',
    'malaysia': 'Malaisie',
    'vietnam': 'Vietnam',
    'thailand': 'Thaïlande',
    'australia': 'Australie',
    'new zealand': 'Nouvelle-Zélande',
    
    # Afrique / Moyen-Orient
    'morocco': 'Maroc',
    'tunisia': 'Tunisie',
    'algeria': 'Algérie',
    'djibouti': 'Djibouti',
    'indonesia': 'Indonésie',
    'egypt': 'Égypte',
    'south africa': 'Afrique du Sud',
    'united arab emirates': 'Émirats Arabes Unis',
    'uae': 'Émirats Arabes Unis',
    'dubai': 'Émirats Arabes Unis',
    'qatar': 'Qatar',
    'saudi arabia': 'Arabie Saoudite',
    
    # Amériques
    'brazil': 'Brésil',
    'argentina': 'Argentine',
    'chile': 'Chili',
    'mexico': 'Mexique',
    'colombia': 'Colombie',
    'canada': 'Canada',
    
    # Europe (Français / Stable)
    'france': 'France',
    'luxembourg': 'Luxembourg',
    'monaco': 'Monaco',
    'belgique': 'Belgique',
    'suisse': 'Suisse',
    'italie': 'Italie',
    'espagne': 'Espagne',
    'allemagne': 'Allemagne',
    'guernsey': 'Guernesey',
    'guernesey': 'Guernesey',
    'tchéquie': 'Tchéquie',
    'tchequie': 'Tchéquie',
    'république tchèque': 'Tchéquie',
    'republique tcheque': 'Tchéquie',
    'indonésie': 'Indonésie',
    'indonesie': 'Indonésie',
    'hong-kong': 'Hong-Kong',
    'hong kong': 'Hong-Kong',
    'corée du sud': 'Corée du Sud',
    'corée Du Sud': 'Corée du Sud',
    'algerie': 'Algérie',
    'algérie': 'Algérie',
    'algeria': 'Algérie',
    'deutschlandweit': 'Allemagne',
    'millénaire 4': 'France',
    'millenaire 4': 'France',
}

def normalize_country(country_raw):
    """
    Normalise le nom d'un pays en français
    """
    if not country_raw:
        return country_raw
    
    country_clean = country_raw.strip()
    
    # Supprimer le préfixe "- " si présent (ex: "- France" → "France")
    if country_clean.startswith('- '):
        country_clean = country_clean[2:].strip()
    
    country_clean = country_clean.lower()
    
    # Supprimer les codes numériques (ex: "91" qui est un département français)
    if country_clean.isdigit() or (len(country_clean) <= 3 and country_clean.isdigit()):
        return "France"  # Les codes numériques courts sont probablement des départements français
    
    # Cas particuliers avant découpe sur virgule
    # Ex: \"Korea, Republic Of\" → Corée du Sud
    if 'korea' in country_clean:
        return 'Corée du Sud'
    if country_clean == 'republic of':
        return 'Corée du Sud'
    # Villes qui ne sont pas des pays (ex: Grandcamp Maisy) → France
    if country_clean == 'grandcamp maisy':
        return 'France'
    
    # Supprimer les codes ISO ou régions si présents après une virgule
    if ',' in country_clean:
        country_clean = country_clean.split(',')[-1].strip()
    
    # Normaliser les variantes courantes
    country_variants = {
        'etats-unis': 'États-Unis',
        'etats unis': 'États-Unis',
        'etats-unis d\'amérique': 'États-Unis',
        'etats unis d\'amérique': 'États-Unis',
        'usa': 'États-Unis',
        'u.s.a': 'États-Unis',
        'corée': 'Corée du Sud',
        'corée du sud': 'Corée du Sud',
        'viet nam': 'Vietnam',
        'vietnam': 'Vietnam',
        'turkiye': 'Turquie',
        'türkiye': 'Turquie',
        'israel': 'Israël',
        'bahamas': 'Bahamas',
        'panama': 'Panama',
        'laos': 'Laos',
        'kuwait': 'Koweït',
        'peru': 'Pérou',
        'qatar': 'Qatar',
    }
    
    # Vérifier les variantes d'abord
    if country_clean in country_variants:
        return country_variants[country_clean]
        
    if country_clean in COUNTRY_MAPPING:
        return COUNTRY_MAPPING[country_clean]
    
    # Si pas dans le mapping, mettre la première lettre en majuscule
    result = country_raw.strip()
    if result.startswith('- '):
        result = result[2:].strip()
    result = result.title()
    
    # Harmoniser "France" (toujours retourner "France" sans préfixe)
    if result.lower() == 'france' or result == '- France':
        return 'France'
    
    return result
