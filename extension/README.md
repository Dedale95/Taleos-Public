# Extension Chrome Taleos

Extension Manifest V3 pour l'automatisation de candidatures bancaires.

**Version** : visible dans la popup (en bas). Incrémenter `version` dans `manifest.json` à chaque release.

## Structure

```
extension/
├── manifest.json          # Config Manifest V3
├── background.js          # Service worker (orchestration)
├── popup/
│   ├── popup.html         # Interface + Login Firebase
│   └── popup.js           # Auth Firebase
├── content/
│   └── taleos-injector.js # Intercepte "Candidater" sur le site Taleos
├── scripts/
│   ├── credit_agricole.js # Automatisation Crédit Agricole
│   └── societe_generale.js# Automatisation SG (placeholder)
└── icons/
```

## Installation

1. Ouvrir Chrome → `chrome://extensions`
2. Activer "Mode développeur"
3. "Charger l'extension non empaquetée"
4. Sélectionner le dossier `extension`

## Mise à jour (quand le code est poussé sur GitHub)

1. Exécuter `./extension/update_extension.sh` (ou `git pull` à la racine)
2. Ouvrir la popup Taleos → cliquer sur **« Mettre à jour l'extension »**

Le bouton recharge l'extension sans passer par `chrome://extensions`.

**Option : rechargement automatique**  
Installez [Extensions Reloader](https://chrome.google.com/webstore/detail/extensions-reloader/fimgfedafeadlieiabdeeaodndnlbhid) et configurez-le pour surveiller le dossier `extension/`. L'extension se rechargera à chaque modification de fichier.

## Utilisation

1. **Connexion** : Cliquer sur l'icône Taleos → Se connecter avec les mêmes identifiants que taleos.co
2. **Connexions bancaires** : Configurer les identifiants banque sur la page Connexions du site Taleos
3. **Postuler** : Sur offres.html ou filtres.html, cliquer sur **Candidater** → l'extension ouvre l'offre et remplit le formulaire

## Workflow

1. Clic "Candidater" sur Taleos → `taleos-injector.js` intercepte
2. Message envoyé au `background.js` (offerUrl, bankId)
3. Background ouvre l'onglet, récupère profil + credentials Firestore
4. Injection du script banque (`credit_agricole.js`, etc.)
5. Le script : login → reset draft si besoin → remplissage → upload CV/LM → RGPD → submit

## Sélecteurs dynamiques (Taleo)

Pour Société Générale / BNP (portails Taleo), utiliser des sélecteurs partiels :

```javascript
document.querySelector('input[id*="personal_info_FirstName"]')
document.querySelector('input[id*="personal_info_LastName"]')
```

## Upload de fichiers

Le CV/LM sont récupérés depuis Firebase Storage (URL) puis convertis en `File` pour l'`input[type=file]` via `DataTransfer`.

## Permissions

- `activeTab`, `scripting`, `storage`, `tabs` : orchestration
- `host_permissions` : Taleos, Firebase, sites carrières des banques

## Flux Crédit Agricole (connexion sur page séparée)

1. **Offre** → Clic "Je postule" → Clic "Connexion" (stocke profil, navigue vers /connexion/)
2. **Page connexion** → `ca-connexion-filler.js` remplit email/mot de passe, envoie
3. **Attente 20s** (comme le notebook Python) → Retour forcé à l’URL de l’offre
4. **Phase 2** → Attente 15s → Clic "Je postule" → Attente formulaire (45s) → Remplissage

## Debug : identifiants non renseignés

1. **Vérifier Firebase** : Connectez-vous à l’extension → cliquez sur « Vérifier identifiants CA ». Si ✅, Firebase est OK. Si ❌, configurez les identifiants sur la page Connexions de Taleos.

2. **Console de la page CA** : Sur la page groupecreditagricole.jobs, F12 → onglet Console. Cherchez les logs `[Taleos CA]` pour voir où ça bloque.

3. **Service Worker** : `chrome://extensions` → Taleos → « Service worker » (lien) → Console. Vérifiez les erreurs de récupération du profil.
