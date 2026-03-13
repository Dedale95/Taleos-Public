# 📥 Guide de téléchargement manuel des logos

Les APIs automatiques ne fonctionnent plus (Clearbit discontinué) et nécessitent des clés API. Voici comment télécharger les logos manuellement.

## 🎯 Méthode recommandée : Google Images

### 1. Crédit Agricole
1. Allez sur [Google Images](https://www.google.com/imghp)
2. Recherchez : `"Crédit Agricole" logo png transparent`
3. Filtrez par : **Taille** > **Grande**, **Type** > **Transparent**
4. Téléchargez un logo de bonne qualité (> 100px)
5. Renommez en : `credit_agricole.png`
6. Placez dans : `HTML/images/logos/credit_agricole.png`

### 2. Société Générale
1. Recherchez : `"Société Générale" logo png transparent`
2. Renommez en : `societe_generale.png`
3. Placez dans : `HTML/images/logos/societe_generale.png`

### 3. Deloitte
1. Recherchez : `"Deloitte" logo png transparent`
2. Renommez en : `deloitte.png` ou `deloitte_france.png`
3. Placez dans : `HTML/images/logos/deloitte.png`

### 4. Groupe BPCE
- Fichier : `bpce.svg` (téléchargé depuis Wikimedia Commons)
- Ou exécuter : `python PYTHON/download_logos_simple.py`

### 5. Autres entreprises
Suivez la même méthode pour :
- **CACEIS** → `caceis.png`
- **LCL** → `lcl.png`
- **Amundi** → `amundi.png`
- **BforBank** → `bforbank.png`
- **Indosuez** → `indosuez.png`
- **UPTEVIA** → `uptevia.png`

## ✅ Vérifications importantes

### Nom de fichier
Les noms doivent correspondre exactement aux noms normalisés :
- ✅ `credit_agricole.png` (avec underscore)
- ❌ `credit-agricole.png` (avec tiret)
- ❌ `Credit_Agricole.png` (avec majuscules)

### Format
- Format accepté : **PNG** (recommandé), JPG, SVG
- Taille recommandée : 128px à 256px
- Fond : **Transparent** (PNG avec alpha) de préférence
- Taille fichier : Entre 10KB et 500KB idéalement

### Emplacement
Les fichiers doivent être dans : `HTML/images/logos/`

## 🔍 Alternatives à Google Images

### Wikipedia
1. Allez sur la page Wikipedia de l'entreprise (ex: [Crédit Agricole](https://fr.wikipedia.org/wiki/Cr%C3%A9dit_Agricole))
2. Cliquez sur le logo en haut à droite
3. Faites un clic droit > "Enregistrer l'image sous..."
4. Convertir en PNG si nécessaire

### Sites officiels (section Presse/Médias)
- [Crédit Agricole - Ressources presse](https://www.credit-agricole.fr/groupe/ressources-presse)
- [Société Générale - Médias](https://www.societegenerale.com/fr/medias)
- [Deloitte - Newsroom](https://www2.deloitte.com/fr/fr/pages/about-deloitte/articles/deloitte-france.html)

### Sites de logos gratuits
- [Logoeps](https://logoeps.com/)
- [Logolynx](https://www.logolynx.com/)
- [Seeklogo](https://seeklogo.com/) (certains gratuits)

## 🛠️ Conversion de format (si nécessaire)

Si vous avez un logo en SVG ou JPG, convertissez-le en PNG :

### Avec un outil en ligne
- [CloudConvert](https://cloudconvert.com/) - Gratuit, convertit SVG/JPG → PNG
- [Zamzar](https://www.zamzar.com/convert/svg-to-png/) - Gratuit

### Avec un logiciel
- **Mac** : Aperçu (Preview) > Exporter > PNG
- **Windows** : Paint > Enregistrer sous > PNG
- **Linux** : GIMP, Inkscape

## 📋 Checklist finale

Avant de tester sur le site, vérifiez :

- [ ] Le fichier est dans `HTML/images/logos/`
- [ ] Le nom correspond exactement (ex: `credit_agricole.png`)
- [ ] Le format est PNG (ou JPG/SVG supporté)
- [ ] La taille du fichier est raisonnable (10KB-500KB)
- [ ] Le logo est lisible à petite taille (40x40px)
- [ ] Le fond est transparent (si possible)

## 🧪 Test

Après avoir ajouté les logos :
1. Rechargez la page "Offres" (F5 ou Cmd+R)
2. Ouvrez la console du navigateur (F12)
3. Regardez l'onglet "Network" pour voir si les logos se chargent
4. Les logos devraient s'afficher automatiquement

## 📞 Besoin d'aide ?

Si un logo ne s'affiche pas :
1. Vérifiez le nom du fichier (doit correspondre au nom normalisé)
2. Vérifiez que le fichier est bien dans `HTML/images/logos/`
3. Vérifiez la console du navigateur (F12) pour les erreurs
4. Consultez `TEST_LOGOS.md` pour plus de détails de debug
