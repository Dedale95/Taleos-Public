import re

CITY_MAPPING = {
    # ========== RÉGION PARISIENNE (IDF) ==========
    'paris': 'Paris',
    'montrouge': 'Montrouge',
    'la defense': 'La Défense',
    'la défense': 'La Défense',
    'puteaux': 'Puteaux',
    'courbevoie': 'Courbevoie',
    'neuilly-sur-seine': 'Neuilly-Sur-Seine',
    'boulogne-billancourt': 'Boulogne-Billancourt',
    'issy-les-moulineaux': 'Issy-Les-Moulineaux',
    'nanterre': 'Nanterre',
    'levallois-perret': 'Levallois-Perret',
    'saint-denis': 'Saint-Denis',
    'fontenay-sous-bois': 'Fontenay-Sous-Bois',
    'vincennes': 'Vincennes',
    'montreuil': 'Montreuil',
    'villejuif': 'Villejuif',
    'ivry-sur-seine': 'Ivry-Sur-Seine',
    'paris montparnasse': 'Paris',
    'guyancourt': 'Guyancourt',
    'saint-quentin-en-yvelines': 'Saint-Quentin-En-Yvelines',
    'saint-quentin en yvelines': 'Saint-Quentin-En-Yvelines',
    'massy': 'Massy',
    'versailles': 'Versailles',
    'évry': 'Evry',
    'evry': 'Evry',
    'cergy': 'Cergy',
    'region parisienne': 'Région Parisienne',
    'ile de france': 'Région Parisienne',
    
    # ========== AUTRES GRANDES VILLES FR ==========
    'lyon': 'Lyon',
    'marseille': 'Marseille',
    'toulouse': 'Toulouse',
    'bordeaux': 'Bordeaux',
    'lille': 'Lille',
    'nice': 'Nice',
    'nantes': 'Nantes',
    'strasbourg': 'Strasbourg',
    'montpellier': 'Montpellier',
    'rennes': 'Rennes',
    'reims': 'Reims',
    'grenoble': 'Grenoble',
    'dijon': 'Dijon',
    'angers': 'Angers',
    'nancy': 'Nancy',
    'orleans': 'Orléans',
    'orléans': 'Orléans',
    'saint-etienne': 'Saint-Etienne',
    'saint-étienne': 'Saint-Etienne',
    
    # ========== VILLES INTERNATIONALES ==========
    'new york': 'New York',
    'london': 'Londres',
    'hong kong': 'Hong-Kong',
    'hong-kong': 'Hong-Kong',
    'singapore': 'Singapour',
    'singapour': 'Singapour',
    'tokyo': 'Tokyo',
    'shanghai': 'Shanghai',
    'frankfurt': 'Francfort',
    'frankfurt am main': 'Francfort',
    'munich': 'Munich',
    'münchen': 'Munich',
    'milan': 'Milan',
    'milano': 'Milan',
    'madrid': 'Madrid',
    'barcelona': 'Barcelone',
    'amsterdam': 'Amsterdam',
    'brussels': 'Bruxelles',
    'bruxelles': 'Bruxelles',
    'geneva': 'Genève',
    'geneve': 'Genève',
    'genève': 'Genève',
    'zurich': 'Zurich',
    'zürich': 'Zurich',
    'lausanne': 'Lausanne',
    'montreal': 'Montréal',
    'montréal': 'Montréal',
    'toronto': 'Toronto',
    'bangalore': 'Bangalore',
    'mumbai': 'Bombay',
    'chennai': 'Chennai',
    'delhi': 'Delhi',
    'dubai': 'Dubaï',
    'dubai/abu dhabi': 'Dubaï',
    'warsaw': 'Varsovie',
    'bucuresti': 'Bucarest',
    'bucharest': 'Bucarest',
    'prague': 'Prague',
    'budapest': 'Budapest',
    'casablanca': 'Casablanca',
    'luxembourg': 'Luxembourg',
    'luxembourg ville': 'Luxembourg',
    'luxembourg-ville': 'Luxembourg',
    'esch-sur-alzette': 'Luxembourg',
    'dublin': 'Dublin',
    'sydney': 'Sydney',
    'melbourne': 'Melbourne',
    'kuala lumpur': 'Kuala Lumpur',
    'putrajaya': 'Kuala Lumpur',
    'aschheim': 'Aschheim',
    'aschheim bei münchen': 'Aschheim',
    
    # ========== ITALIE - Harmonisation villes/provinces ==========
    'torino': 'Turin',
    'turin': 'Turin',
    'parma': 'Parma',
    'parma e provincia': 'Parma',
    'milano e provincia': 'Milan',
    'fidenza': 'Fidenza',
    'verona': 'Verona',
    'verona e provincia': 'Verona',
    'vicenza': 'Vicenza',
    'vicenza e provincia': 'Vicenza',
    'piacenza': 'Piacenza',
    'piacenza e provincia': 'Piacenza',
    'pordenone': 'Pordenone',
    'provincia di pordenone': 'Pordenone',
    'udine': 'Udine',
    'provincia di udine': 'Udine',
    'provincia di udine e pordenone': 'Udine',
    'pavia': 'Pavia',
    'provincia di pavia': 'Pavia',
    'genova': 'Genova',
    'genoa': 'Genova',
    'provincia di genova': 'Genova',
    'modena': 'Modena',
    'provincia di modena': 'Modena',
    'cuneo': 'Cuneo',
    'province di cuneo': 'Cuneo',
    'province di cuneo e verbano': 'Cuneo',
    'verbano': 'Verbano',
    'mantova': 'Mantova',
    'mantua': 'Mantova',
    'province di mantova': 'Mantova',
    'cremona': 'Cremona',
    'province di cremona': 'Cremona',
    'province di mantova e cremona': 'Mantova',
    'sondrio': 'Sondrio',
    'sondrio e valtellina': 'Sondrio',
    'lecco': 'Lecco',
    'lecco e valtellina': 'Lecco',
    'roma': 'Rome',
    'rome': 'Rome',
    'bologna': 'Bologne',
    'firenze': 'Florence',
    'florence': 'Florence',
    'napoli': 'Naples',
    'naples': 'Naples',
}

def normalize_city(city_raw):
    """
    Normalise un nom de ville selon les règles de mapping.
    Retourne None si la valeur ne semble pas être une ville valide.
    """
    if not city_raw:
        return None
    
    # Nettoyer et normaliser
    city_clean = city_raw.strip().lower()
    
    # Liste des pays connus à rejeter (ne doivent pas être traités comme villes)
    known_countries_lower = {
        'france', 'inde', 'india', 'japon', 'japan', 'pologne', 'poland', 'roumanie', 'romania', 'chine', 'china', 'corée', 'corée du sud', 'korea', 'south korea',
        'italie', 'italy', 'allemagne', 'germany', 'espagne', 'spain', 'portugal', 'belgique', 'belgium', 'suisse', 'switzerland', 'luxembourg',
        'pays-bas', 'royaume-uni', 'united kingdom', 'états-unis', 'etats-unis', 'usa', 'united states',
        'canada', 'singapour', 'singapore', 'hong-kong', 'hong kong', 'thailande', 'thaïlande', 
        'thailand', 'malaisie', 'malaysia', 'australie', 'australia', 'nouvelle-zélande', 
        'new zealand', 'brésil', 'brazil', 'argentine', 'argentina', 'chili', 'chile', 
        'mexique', 'mexico', 'colombie', 'colombia', 'afrique du sud', 'south africa',
        'égypte', 'egypt', 'maroc', 'morocco', 'tunisie', 'tunisia', 'algérie', 'algeria',
        'grèce', 'greece', 'turquie', 'turkey', 'russie', 'russia', 'ukraine',
        'hongrie', 'hungary', 'tchéquie', 'czech republic', 'slovaquie', 'slovakia',
        'autriche', 'austria', 'irlande', 'ireland', 'norvège', 'norway', 'suède', 'sweden',
        'finlande', 'finland', 'danemark', 'denmark', 'pays-bas', 'netherlands', 'hollande'
    }
    
    # Rejeter si c'est un pays
    if city_clean in known_countries_lower:
        return None
    
    # EN PRIORITÉ : Extraire la ville depuis un format "Code postal + Ville" AVANT les autres vérifications
    # Pattern pour codes postaux suivis d'une ville (allemand: 5 chiffres, suisse: 4 chiffres, français: 5-6 chiffres)
    # On cherche à la fin de la chaîne pour avoir la ville finale
    postal_city_pattern = r'\b(\d{4,6})\s+([A-Za-zäöüÄÖÜßÉéèêëàáâãäåçñ\-]+(?:\s+[A-Za-zäöüÄÖÜßÉéèêëàáâãäåçñ\-]+)?)\s*$'
    postal_match = re.search(postal_city_pattern, city_clean)
    postal_extracted_city = None
    if postal_match:
        # Si on trouve un code postal suivi d'une ville, prendre la ville
        postal_extracted_city = postal_match.group(2).strip()
        if postal_extracted_city:
            city_clean = postal_extracted_city
            # Si on a extrait une ville depuis un code postal, on peut skip les vérifications d'adresse
            # On passe directement au mapping et nettoyage final
    
    # Supprimer les adresses complètes (Road, Street, Av., #, Floor, etc.)
    # Détecter si c'est une adresse complète
    address_patterns = [
        r'^\d+\s+(road|street|avenue|av\.?|boulevard|blvd|drive|dr|lane|ln|way|plaza|tower|building|allée|allée|chemin|rue)',
        r'^\d+\s+allée\s+',  # Ex: "9 Allée Scheffer"
        r'^\d+\s+allee\s+',  # Ex: "9 Allee Scheffer" (sans accent)
        r'#\d+',
        r'\d+th\s+floor',
        r'\d+st\s+floor',
        r'\d+nd\s+floor',
        r'\d+rd\s+floor',
        r'\d+º\s+floor',
        r'\d+\s+floor',
        r'^\d+\s+',  # Commence par un nombre seul (ex: "2 Central Boulevard")
        r'capital tower',
        r'\bfloor\b',
        r'av\.\s+[a-z]',  # Ex: "Av. Linares", "Av. Miguel"
        r'gmbh',
        r'co\.\s*kg',
        r'leasing',
        r'factoring',
        r'\d{4,}\s+',  # Codes postaux longs au début
        r'\b\d{4,}\s+[a-z]',  # Code postal suivi d'une ville (ex: "1010 Lausanne" -> rejeter)
        r'chemin\s+de\s+[a-z]+\s+\d+',  # Ex: "Chemin De Bérée 38"
        r'einsteinring\s+\d+',  # Ex: "Einsteinring 30"
        r'building|bldg|tower',  # Mots-clés de bâtiments
        r'sumitomo\s+bldg',  # Ex: "Shiodome Sumitomo Bldg. 14F"
        r'\d+f\b',  # Étage (ex: "14F")
        r'metro\s+park',  # Ex: "Metro Park"
        r'allée\s+scheffer',  # Ex: "Allée Scheffer"
        r'allee\s+scheffer',  # Ex: "Allee Scheffer" (sans accent)
    ]
    
    # Mots-clés de noms d'entreprises à exclure
    company_keywords = [
        'crédit agricole', 'leasing', 'factoring', 'gmbh', 'co.', 's.a.',
        'indosuez', 'amundi', 'caceis', 'lcl', 'bforbank', 'merca',
        'leasing & factoring', 'leasing &', '& factoring'
    ]
    
    is_address = any(re.search(pattern, city_clean, re.IGNORECASE) for pattern in address_patterns)
    is_company = any(keyword in city_clean for keyword in company_keywords)
    
    # Si on a déjà extrait une ville depuis un code postal, skip les vérifications d'adresse
    if postal_extracted_city:
        # On a déjà la ville, continuer avec le nettoyage final
        pass
    elif is_address or is_company:
        # Extraire juste le nom de ville depuis l'adresse
        # Chercher après les mots-clés d'adresse
        # Ex: "168 Robinson Road #23-03 Capital Tower Singapore" -> "Singapore"
        # On cherche généralement à la fin
        
        # Si contient des mots-clés de ville connus, les extraire
        known_city_keywords = ['singapore', 'singapour', 'hong-kong', 'hong kong', 'madrid', 'barcelone', 
                               'barcelona', 'lisbonne', 'lisboa', 'coruña', 'a coruña', 'lausanne',
                               'zurich', 'genève', 'geneva', 'paris', 'london', 'londres']
        extracted_city = None
        for keyword in known_city_keywords:
            if keyword in city_clean:
                # Extraire le mot-clé et quelques mots autour
                match = re.search(rf'\b{re.escape(keyword)}\b', city_clean)
                if match:
                    # Prendre le dernier mot significatif (généralement la ville)
                    parts = city_clean.split()
                    # Chercher le mot-clé dans les parties
                    for i, part in enumerate(parts):
                        if keyword in part:
                            # Prendre cette partie et éventuellement la suivante
                            if i < len(parts) - 1 and parts[i+1] not in ['tower', 'building', 'road', 'street', 'park']:
                                extracted_city = ' '.join(parts[i:i+2])
                            else:
                                extracted_city = parts[i]
                            break
                    if extracted_city:
                        city_clean = extracted_city
                        break
        
        # Si on n'a pas trouvé de ville connue et que c'est clairement une adresse/entreprise, rejeter
        if not extracted_city and (is_address or is_company):
            # Essayer une dernière fois d'extraire quelque chose de valide
            parts = [p for p in city_clean.split() if not re.match(r'^\d+', p) and p not in ['road', 'street', 'av.', 'boulevard', 'floor', 'tower', 'building', '#', 'park', 'bldg', 'bldg.']]
            if parts and len(parts) <= 2:  # Si seulement 1-2 mots restants
                city_clean = ' '.join(parts)
            else:
                # Si trop de mots ou suspects, rejeter
                return None
    
    # Supprimer les parenthèses et leur contenu (ex: "Casablanca (Maroc)")
    city_clean = re.sub(r'\(.*?\)', '', city_clean).strip()
    
    # EN PRIORITÉ : Extraire la ville depuis un format "Code postal + Ville" (ex: "85609 Aschheim", "1010 Lausanne")
    # Pattern pour codes postaux suivis d'une ville (allemand: 5 chiffres, suisse: 4 chiffres, français: 5-6 chiffres)
    # On cherche à la fin de la chaîne pour avoir la ville finale
    postal_city_pattern = r'\b(\d{4,6})\s+([A-Za-zäöüÄÖÜßÉéèêëàáâãäåçñ\-]+(?:\s+[A-Za-zäöüÄÖÜßÉéèêëàáâãäåçñ\-]+)?)\s*$'
    postal_match = re.search(postal_city_pattern, city_clean)
    if postal_match:
        # Si on trouve un code postal suivi d'une ville, prendre la ville
        extracted_city = postal_match.group(2).strip()
        if extracted_city:
            # Si on a extrait une ville depuis un code postal, on peut l'utiliser directement
            city_clean = extracted_city.lower()
            # Ne pas continuer avec les autres vérifications d'adresse si on a trouvé un code postal + ville
            # On passe directement au mapping et nettoyage final
    
    # Supprimer les codes postaux restants (si pas déjà extrait par le pattern ci-dessus)
    if not postal_match:  # Seulement si on n'a pas déjà extrait depuis un code postal
        city_clean = re.sub(r'\b\d{5,6}\b', '', city_clean).strip()
    
    # Supprimer les caractères spéciaux d'adresses restants
    city_clean = re.sub(r'[#º]', '', city_clean).strip()
    
    # Gérer les cas spéciaux avec "/" ou ","
    if '/' in city_clean:
        city_clean = city_clean.split('/')[0].strip()
    if ',' in city_clean:
        city_clean = city_clean.split(',')[0].strip()
    
    # Supprimer les mentions type "avec des déplacements..." ou "– Campus"
    city_clean = re.sub(r' (avec|des|–|—|ou|et).*$', '', city_clean).strip()
    
    # Supprimer les espaces multiples
    city_clean = re.sub(r'\s+', ' ', city_clean).strip()
    
    # Appliquer le mapping
    if city_clean in CITY_MAPPING:
        return CITY_MAPPING[city_clean]
    
    # Vérifications finales : rejeter les valeurs suspectes
    # Si la valeur est très longue (>30 caractères), c'est probablement une adresse complète
    if len(city_clean) > 30:
        return None
    
    # Si la valeur contient encore des chiffres isolés ou des mots-clés suspects, rejeter
    suspicious_patterns = [
        r'^\d+$',  # Uniquement des chiffres
        r'^\d+\s+\w+$',  # Nombre suivi d'un mot (ex: "29Th Floor")
        r'^\d+[a-z]?$',  # Nombre seul avec lettre optionnelle (ex: "30", "14F")
        r'\d{4,}',  # Code postal long
    ]
    if any(re.search(pattern, city_clean) for pattern in suspicious_patterns):
        return None
    
    # Rejeter les mots isolés qui ne sont pas des villes connues
    # Mots-clés suspects qui ne devraient pas être des villes
    invalid_standalone_words = ['central', 'boulevard', 'metro', 'park', 'einsteinring', 
                                'allée', 'chemin', 'allee', 'scheffer', 'floor', 'bldg',
                                'building', 'tower', 'road', 'street', 'avenue']
    if city_clean in invalid_standalone_words:
        return None
    
    # Appliquer le mapping
    if city_clean in CITY_MAPPING:
        return CITY_MAPPING[city_clean]
    
    # Si pas dans le mapping, vérifier si ça ressemble à une ville valide
    # Rejeter si ça contient des mots-clés suspects
    invalid_keywords = ['provincia', 'province', 'province di', 'valtellina', 'leasing', 'factoring',
                       'central', 'boulevard', 'metro', 'park', 'einsteinring', 'scheffer', 'bldg']
    if any(keyword in city_clean for keyword in invalid_keywords):
        return None
    
    # Si pas dans le mapping et que c'est un seul mot suspect, rejeter
    if ' ' not in city_clean and city_clean not in CITY_MAPPING:
        # Vérifier si c'est un mot suspect
        if city_clean in invalid_standalone_words:
            return None
    
    # Si pas dans le mapping, retourner en Title Case
    # Regex pour mettre en majuscule après un tiret
    city_title = city_clean.title()
    result = re.sub(r'-([a-z])', lambda m: '-' + m.group(1).upper(), city_title)
    
    # Dernière vérification : si le résultat final est trop long ou suspect, rejeter
    if len(result) > 30 or re.search(r'\d{4,}', result):
        return None
    
    return result
