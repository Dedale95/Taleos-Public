# Flux candidature Crédit Agricole – Testé avec Browser MCP

Documentation du parcours candidature de A à Z sur groupecreditagricole.jobs, validé via le MCP **cursor-ide-browser** (mars 2026).

## 1. Récupération du profil utilisateur (Firebase)

Pour utiliser les identifiants et le profil (CV, LM, etc.) de l’utilisateur **thibault.giraudet@outlook.com** :

```bash
export GOOGLE_APPLICATION_CREDENTIALS=/chemin/vers/serviceAccountKey.json
python PYTHON/fetch_firebase_profile.py --email thibault.giraudet@outlook.com --bank credit_agricole
```

- **Prérequis** : clé de compte de service Firebase (JSON) ; ne jamais commiter ce fichier.
- **Banque** : `--bank credit_agricole` pour le document `career_connections/credit_agricole` (email/mot de passe CA).
- Sans `GOOGLE_APPLICATION_CREDENTIALS`, le script échoue et l’extension ne peut pas remplir automatiquement la connexion ni le formulaire.

## 2. Parcours testé (de A à Z)

### Étape 1 – Page offre

- **URL type** : `https://groupecreditagricole.jobs/fr/nos-offres-emploi/577-170470-127-cdi---analyste-risque-credit-senior-hf-reference--2025-101695--/`
- **Bandeau cookies** : boutons "Accepter" et **"Refuser"** (l’extension peut fermer le bandeau avec l’un ou l’autre).
- **Bouton "Je postule"** : présent sur la page (ex. `button` avec texte "Je postule", ou `button.cta.primary[data-popin="popin-application"]`).

### Étape 2 – Modale « Comment souhaitez-vous postuler ? »

- Après clic sur "Je postule", une **modale** s’ouvre avec :
  - **Postuler en tant qu’invité**
  - **Connexion** (lien vers la page de connexion) ← utilisé par l’extension
  - **Je crée mon compte**
- **Sélecteur utilisé** : dans `#popin-application`, lien `a[href*="connexion"]` ou `a.cta.secondary.arrow[href*="connexion"]`.
- Clic sur "Connexion" → **navigation vers** `https://groupecreditagricole.jobs/fr/connexion/`.

### Étape 3 – Page connexion

- **URL** : `https://groupecreditagricole.jobs/fr/connexion/`
- **Champs** (accessibles par id ou par rôle) :
  - Adresse e-mail * → `#form-login-email` ou `input[type="email"]`
  - Mot de passe * → `#form-login-password` ou `input[type="password"]`
  - Bouton **Connexion** → `#form-login-submit` ou `button[type="submit"]`
- **Message** : « Votre compte sera bloqué suite à 5 essais de mot de passe erronés » → ne pas soumettre de faux mots de passe en test.
- L’extension remplit email/mot de passe depuis Firebase (`auth_email`, `auth_password` décodé) puis clique sur le bouton Connexion.

### Étape 4 – Après connexion réussie

- **Comportement attendu** : redirection vers l’offre (ou page d’accueil) ; l’extension recharge l’offre et ré-injecte le script (phase 2).
- **Phase 2** : sur la page offre, attente fin du chargement, fermeture cookies si besoin, clic sur "Je postule" à nouveau.
- **Phase 3** : formulaire de candidature (4 étapes : Infos, Documents, Profil, Formations) → remplissage depuis le profil Firebase, RGPD, envoi.

### Étape 5 – Succès

- **Page** : URL contenant `candidature-validee` ou message du type « Votre candidature a été envoyée avec succès ».
- L’extension notifie le backend (tuile Taleos, candidature_success).

## 3. Points de vigilance pour la robustesse

| Point | Comportement actuel | Recommandation |
|-------|---------------------|----------------|
| Cookies | Boutons "Refuser" / "Accepter" ; sélecteurs `rgpd-btn-refuse`, `rgpd-btn-accept` ou texte | Privilégier "Refuser" en premier pour cohérence et moindre tracking. |
| Popin | Lien "Connexion" dans `#popin-application` | Garder les fallbacks `a[href*="connexion"]`, `a[href*="login"]`. |
| Formulaire connexion | IDs `form-login-email`, `form-login-password`, `form-login-submit` | Conserver les fallbacks `input[type="email"]`, `input[type="password"]`, `button[type="submit"]` (ca-connexion-filler.js). |
| Hydration | Attente `waitForFormReady` / `waitForLoadingComplete` avant remplissage | Conserver les délais et vérifications de stabilité. |
| Fichiers (CV/LM) | Upload via `fetch_storage_file` (Firebase Storage) | Vérifier que les chemins `cv_storage_path` / `letter_storage_path` sont bien renseignés dans le profil. |

## 4. Tester une candidature complète

1. **Avoir le profil Firebase** : ex. `fetch_firebase_profile.py --email thibault.giraudet@outlook.com --bank credit_agricole` avec une clé de compte de service.
2. **Connexion CA** : dans Taleos, page Connexions, lier le compte Crédit Agricole (email + mot de passe).
3. **Lancer une candidature** depuis la page offres Taleos (bouton candidater sur une offre CA) : l’extension ouvre l’offre, gère cookies, "Je postule" → Connexion → remplissage connexion → retour offre → formulaire → envoi.

Pour un test **sans soumettre** (éviter tout blocage) : aller jusqu’à la page connexion et ne pas cliquer sur "Connexion" après avoir rempli (ou utiliser un compte de test dédié).
