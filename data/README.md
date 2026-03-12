# Données partagées

## deloitte-institutions.json

Liste des établissements reconnus par le formulaire Deloitte (Workday). Elle alimente le champ « Établissement ou université » du profil Taleos pour n’autoriser que des valeurs acceptées par Deloitte.

**Mise à jour de la liste :**

1. Ouvrir une candidature Deloitte et aller à l’étape 2 (Mon expérience).
2. Ouvrir la popup de l’extension Taleos → carte « Établissements Deloitte ».
3. Cliquer sur « Récupérer la liste des établissements », attendre la fin.
4. Cliquer sur « Copier le JSON ».
5. Remplacer le contenu de `data/deloitte-institutions.json` par ce JSON (tableau de chaînes).

Format attendu : `["Autre établissement", "ESCP Europe", "HEC Paris", ...]`
