# Analyse de la structure du site BNP Paribas - Offres d'emploi

## Date de l'analyse
13 mars 2026

## URL de la page de liste
https://group.bnpparibas/emploi-carriere/toutes-offres-emploi?q=

## URL de la première offre analysée
https://group.bnpparibas/emploi-carriere/offre-emploi/analista-remarketing-senior

**Href de la première offre:** `/emploi-carriere/offre-emploi/analista-remarketing-senior`

---

## 1. STRUCTURE DE LA PAGE DE LISTE

### Informations générales
- **Titre de la page:** "Toutes nos offres d'emploi (CDI, CDD, VIE, stage) - BNP Paribas"
- **Nombre total d'offres:** 3 702 offres dans 49 zones géographiques
- **Pagination:** 371 pages (10 offres par page)

### Structure des liens d'offres d'emploi

Les offres d'emploi sont présentées sous forme de liens avec la structure suivante :

#### Exemples de liens identifiés (refs du snapshot) :
1. **ref: e27** - `CDI Analista Remarketing Senior São Paulo, État de São Paulo, Brésil`
   - Href: `/emploi-carriere/offre-emploi/analista-remarketing-senior`
   
2. **ref: e28** - `Stage Beca ADE (Incorporación en Junio) Madrid, Communauté de Madrid, Espagne`
   
3. **ref: e29** - `Beca ADE (Incorporación en Junio Madrid, Communauté de Madrid, Espagne`
   
4. **ref: e30** - `CDI Старший персональний консультант фінансовий з індивідуального бізнесу Kharkiv, Oblast de Kharkiv, Ukraine`

5. **ref: e32** - `Adviseur Bank & Verzekeringen - Regio Grimbergen Grimbergen, Bruxelles, Belgique`

6. **ref: e33** - `Stage Sales Assistant Intern Lisbonne, Lisbonne, Portugal`

7. **ref: e34** - `Stage Data Analyst Trainee Lisbonne, Lisbonne, Portugal`

### Pattern des URLs d'offres
- Format: `/emploi-carriere/offre-emploi/{slug-de-l-offre}`
- Le slug est dérivé du titre de l'offre en minuscules avec des tirets

### Sélecteurs CSS potentiels pour la page de liste

Basé sur l'analyse du snapshot, les sélecteurs suivants devraient être testés :

```css
/* Liens vers les offres d'emploi */
a[href*="/emploi-carriere/offre-emploi/"]
a[href*="/offre-emploi/"]

/* Conteneur des offres */
[role="listitem"] a[href*="/offre-emploi/"]

/* Titres des offres (h3) */
h3[class*=""] /* À affiner avec inspection du DOM */
```

### Éléments de pagination

- **Navigation de pagination:** `role: navigation, name: Pagination`
- **Liens de pagination identifiés:**
  - Page 1 (ref: e130)
  - Page 2 (ref: e131)
  - ... (ref: e132)
  - Page 370 (ref: e133)
  - Page 371 (ref: e134)
  - Suivant (ref: e135)

### Filtres disponibles
- **Contrat** (ref: e17) - Tab
- **Localisation** (ref: e18) - Tab
- **Métiers** (ref: e19) - Tab
- **Afficher plus de filtres** (ref: e20) - Button

### Compteur d'offres
- **Heading:** "Nous avons 3 702 offres dans 49 zones géographiques" (ref: e105, level: 2)

---

## 2. STRUCTURE DE LA PAGE DE DÉTAIL

### Informations de l'offre analysée
- **Titre:** "Analista Remarketing Senior"
- **Type de contrat:** CDI (Permanent)
- **Horaires:** Temps plein
- **Localisation:** São Paulo, État de São Paulo, Brésil
- **Entreprise:** Arval (Groupe BNP Paribas)
- **Référence:** 111111111117223
- **Métier:** Développement commercial

### Structure des éléments clés

#### 1. Titre principal
- **Element:** `<h1>` (ref: e78, level: 1)
- **Texte:** "Analista Remarketing Senior"
- **Classe:** À déterminer via inspection du DOM

#### 2. Informations de contrat
- **Type de contrat:** "CDI (Permanent)" (ref: e17 - link, ref: e80 - listitem)
- **Horaires:** "Temps plein" (ref: e81 - listitem)
- **Localisation:** "São Paulo, État de São Paulo, Brésil" (ref: e20 - link, ref: e82 - listitem)

#### 3. Métier
- **Link:** "Développement commercial" (ref: e19)

#### 4. Référence de l'offre
- **Texte:** "111111111117223"
- **Label:** "RÉFÉRENCE"

#### 5. Boutons d'action
- **"Postuler"** (ref: e16 et e21)
- **"Retour à la liste des offres"** (ref: e15)

#### 6. Sections de contenu

Les sections suivantes ont été identifiées dans le snapshot :

1. **Contexte de l'entreprise** (ref: e83)
   - Description d'Arval

2. **Diversité et inclusion** (ref: e84)
   - "Na Arval, a diversidade, a equidade e a inclusão são fatores essenciais de nossa estratégia."

3. **Contexte de la zone** (ref: e85)
   - "Contexto da área:"

4. **Description du poste** (ref: e86)
   - Responsabilités de la zone Remarketing

5. **Activités** (ref: e87)
   - Liste des activités (refs: e88-e93)

6. **Compétences comportementales** (ref: e95)
   - Liste des compétences (refs: e96-e102)

7. **Exigences techniques** (ref: e103)
   - Liste des exigences (refs: e104-e108)

#### 7. Section "Arval"
- **Heading:** "Arval" (ref: e109, level: 3)
- **Description:** (ref: e110)

#### 8. Offres similaires
- **Heading:** "Ces autres offres vous intéressent-elles ?" (ref: e111, level: 2)
- **Offres listées:**
  - (Senior) Sales & Relationship Manager Erlebniswelt Consume (all genders) (ref: e23)
  - (Senior) Relationship Manager – (all genders) in Growth Capital and Solutions Germany (ref: e24)
  - SENIOR BANKER (ref: e25)

### Sélecteurs CSS potentiels pour la page de détail

```css
/* Titre de l'offre */
h1

/* Type de contrat */
a[href*="CDI"], a[href*="CDD"], a[href*="Stage"], a[href*="VIE"]
/* ou via listitem contenant "CDI", "CDD", etc. */

/* Localisation */
a[href*="localisation"] /* À vérifier */
[role="listitem"]:has-text("São Paulo") /* Approche alternative */

/* Métier */
a[href*="metier"] /* À vérifier */

/* Référence */
/* Chercher un élément contenant "RÉFÉRENCE" suivi d'un nombre */

/* Bouton Postuler */
a[href*="postuler"]
button:has-text("Postuler")

/* Sections de contenu */
/* Les sections semblent être dans des éléments avec role="listitem" ou des paragraphes */
```

---

## 3. SCRIPTS JAVASCRIPT POUR L'ANALYSE

### Script pour la page de liste

```javascript
(function() {
    const result = {};
    
    // Find all job cards/links
    const jobLinks = document.querySelectorAll('a[href*="/offre-emploi/"], a[href*="/emploi-carriere/offre"]');
    result.jobLinks = Array.from(jobLinks).slice(0, 5).map(a => ({
        href: a.href,
        text: a.textContent.trim().substring(0, 200),
        className: a.className,
        parentClass: a.parentElement?.className,
        innerHTML: a.innerHTML.substring(0, 500)
    }));
    
    // Pagination
    const paginationLinks = document.querySelectorAll('a[href*="page="], [class*="pager"], [class*="pagination"]');
    result.pagination = Array.from(paginationLinks).slice(0, 10).map(el => ({
        href: el.href || '',
        text: el.textContent.trim(),
        className: el.className,
        tagName: el.tagName
    }));
    
    // Total count
    const countElements = document.querySelectorAll('[class*="count"], [class*="total"], [class*="result"]');
    result.counts = Array.from(countElements).slice(0, 5).map(el => ({
        text: el.textContent.trim().substring(0, 200),
        className: el.className
    }));
    
    // Contract type badges
    const badges = document.querySelectorAll('[class*="badge"], [class*="tag"], [class*="label"], [class*="contract"]');
    result.badges = Array.from(badges).slice(0, 10).map(el => ({
        text: el.textContent.trim(),
        className: el.className
    }));
    
    return JSON.stringify(result, null, 2);
})()
```

### Script pour la page de détail

```javascript
(function() {
    const result = {};
    
    // Title
    const h1 = document.querySelector('h1');
    result.title = h1 ? {text: h1.textContent.trim(), tagName: h1.tagName, className: h1.className} : null;
    
    // All meta-like info sections
    const allDivs = document.querySelectorAll('div, span, p, li');
    const metaInfo = [];
    allDivs.forEach(el => {
        const text = el.textContent.trim();
        if (text.length > 3 && text.length < 200 && !el.children.length) {
            const keywords = ['CDI', 'CDD', 'Stage', 'Alternance', 'VIE', 'Temps plein', 'Temps partiel', 'Bac', 'Master', 'Référence', 'Date', 'Métier', 'métier'];
            if (keywords.some(k => text.includes(k))) {
                metaInfo.push({text: text, tagName: el.tagName, className: el.className, parentClass: el.parentElement?.className});
            }
        }
    });
    result.metaInfo = metaInfo.slice(0, 30);
    
    // Look for structured data
    const scripts = document.querySelectorAll('script[type="application/ld+json"]');
    result.jsonLd = [];
    scripts.forEach(s => {
        try { result.jsonLd.push(JSON.parse(s.textContent)); } catch(e) {}
    });
    
    // All links on the page
    const links = document.querySelectorAll('a[href*="emploi-carriere"]');
    result.careerLinks = Array.from(links).slice(0, 10).map(a => ({href: a.href, text: a.textContent.trim().substring(0, 100)}));
    
    // Sections with headers
    const headers = document.querySelectorAll('h2, h3, h4');
    result.headers = Array.from(headers).map(h => ({text: h.textContent.trim(), tag: h.tagName, className: h.className}));
    
    // Location info
    const locationElements = document.querySelectorAll('[class*="location"], [class*="lieu"], [class*="place"]');
    result.locationElements = Array.from(locationElements).slice(0, 5).map(el => ({text: el.textContent.trim(), className: el.className}));
    
    return JSON.stringify(result, null, 2);
})()
```

---

## 4. OBSERVATIONS ET RECOMMANDATIONS

### Observations clés

1. **Structure des URLs:** Les URLs des offres suivent un pattern prévisible basé sur le slug du titre
2. **Pagination:** Le site utilise une pagination classique avec des liens numérotés
3. **Accessibilité:** Le site utilise des attributs ARIA (roles, refs) ce qui facilite la navigation programmatique
4. **Multilingue:** Les offres sont disponibles dans plusieurs langues (français, espagnol, portugais, ukrainien, etc.)

### Recommandations pour le scraping

1. **Sélecteurs à utiliser:**
   - Pour les liens d'offres: `a[href*="/emploi-carriere/offre-emploi/"]`
   - Pour la pagination: Chercher les liens dans `[role="navigation"][name="Pagination"]`
   - Pour le compteur: Chercher le h2 contenant "offres dans"

2. **Gestion de la pagination:**
   - Le site affiche 10 offres par page
   - Il y a 371 pages au total
   - Le bouton "Voir plus d'offres d'emploi" (ref: e35) peut charger plus d'offres dynamiquement

3. **Extraction des données:**
   - Le titre est dans un `<h1>`
   - Les métadonnées (contrat, localisation, métier) sont dans des liens ou des listitems
   - La référence est affichée avec le label "RÉFÉRENCE"
   - La description est dans plusieurs sections avec des rôles "listitem"

4. **Points d'attention:**
   - Le site peut utiliser du JavaScript pour charger les offres dynamiquement
   - Il faut attendre que la page soit complètement chargée avant d'extraire les données
   - Les classes CSS peuvent changer, privilégier les sélecteurs basés sur les attributs href et les rôles ARIA

---

## 5. DONNÉES BRUTES DES SNAPSHOTS

### Snapshot de la page de liste (extrait)

```yaml
- role: link
  name: CDI Analista Remarketing Senior São Paulo, État de São Paulo, Brésil
  ref: e27

- role: link
  name: Stage Beca ADE (Incorporación en Junio) Madrid, Communauté de Madrid, Espagne
  ref: e28

- role: link
  name: Beca ADE (Incorporación en Junio Madrid, Communauté de Madrid, Espagne
  ref: e29

- role: navigation
  name: Pagination
  ref: e129

- role: heading
  name: Nous avons 3 702 offres dans 49 zones géographiques
  ref: e105
  level: 2
```

### Snapshot de la page de détail (extrait)

```yaml
- role: heading
  name: Analista Remarketing Senior
  ref: e78
  level: 1

- role: listitem
  name: CDI ( Permanent )
  ref: e80

- role: listitem
  name: Temps plein
  ref: e81

- role: listitem
  name: São Paulo, État de São Paulo, Brésil
  ref: e82

- role: link
  name: Développement commercial
  ref: e19

- role: link
  name: Postuler
  ref: e16
```

---

## CONCLUSION

Le site BNP Paribas utilise une structure relativement standard pour ses offres d'emploi, avec des URLs prévisibles et une bonne accessibilité grâce aux attributs ARIA. Les sélecteurs CSS basés sur les attributs `href` contenant `/emploi-carriere/offre-emploi/` devraient être fiables pour identifier les liens vers les offres.

Pour une extraction complète, il est recommandé de :
1. Parcourir toutes les pages de pagination
2. Extraire les URLs de toutes les offres
3. Visiter chaque page de détail pour extraire les informations complètes
4. Gérer les cas où le contenu est chargé dynamiquement via JavaScript

**Note importante:** Les scripts JavaScript fournis dans ce document n'ont pas pu être exécutés directement dans la console du navigateur via les outils disponibles. Pour obtenir les résultats bruts de ces scripts, il faudrait les exécuter manuellement dans la console du navigateur ou utiliser un outil de scraping comme Selenium ou Puppeteer.
