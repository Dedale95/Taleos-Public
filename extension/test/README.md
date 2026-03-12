# Test du formulaire Deloitte (étape 2)

Avant de modifier `content/deloitte-careers-filler.js` (établissement, domaine, années, etc.), **tester sur la page fixture** pour vérifier que la saisie fonctionne.

## Marche à suivre

1. **Servir la fixture en local** (à la racine du dépôt) :
   ```bash
   python3 -m http.server 8000
   ```

2. **Ouvrir dans le navigateur** (avec l’extension Taleos chargée) :
   ```
   http://localhost:8000/extension/test/deloitte-step2-fixture.html
   ```

3. **Cliquer sur** « Remplir avec profil test (ESCP Europe, 2018) ».

4. **Vérifier** :
   - **Établissement ou université** = `ESCP Europe`
   - **Année de début** = `2014` (2018 - 4)
   - **Année de fin** = `2018`
   - Domaine d’études reste vide (profil test sans `study_domain`).

5. **Ensuite seulement** modifier le script si besoin, puis refaire un test sur la fixture avant de commit.

## Pourquoi

La page fixture reproduit la structure du formulaire Workday (labels, `role="spinbutton"`, aria-labels « Année de début » / « Année de fin »). Tester ici évite de pousser du code sans l’avoir confronté à une page réelle (ou à une copie locale contrôlée).
