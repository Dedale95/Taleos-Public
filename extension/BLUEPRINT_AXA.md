# Blueprint AXA

Flux confirmé en live sur `careers.axa.com` et `careers-fr-axa.icims.com`, avec candidature réelle soumise sur l'offre `4271`.

## Pages

- `offer_public`
  - URL type : `https://careers.axa.com/careers-home/jobs/<jobId>`
  - signal : bouton `POSTULER`
  - redirection utile : lien réel vers `https://careers-fr-axa.icims.com/jobs/<jobId>/login`

- `login_identifier`
  - URL type : `https://login.icims.eu/u/login/identifier?...`
  - point d'entrée robuste utilisé par Taleos :
    - `https://careers-fr-axa.icims.com/jobs/4271/login?loginOnly=1&in_iframe=1`
  - champs :
    - `input#username`
    - bouton `Continue`

- `login_password`
  - URL type : `https://login.icims.eu/u/login/password?...`
  - champs :
    - `input[type="password"]`
    - bouton `LOG IN`
  - erreur métier confirmée :
    - `Wrong username or password`

- `profile`
  - URL type :
    - outer : `/candidate?mode=apply...`
    - inner frame : `/candidate?mode=apply...&in_iframe=1`
  - signal :
    - `Step 1 of 3. Profil du candidat`
  - point critique :
    - un bandeau cookies AXA peut bloquer la page outer
    - CTA à cliquer : `Continuer sans accepter`

- `questions`
  - URL type : `/questions?in_iframe=1`
  - signal :
    - `Step 2 of 3. Questions supplémentaires requises pour le poste`
  - CTA final observé sur cette offre :
    - `#quesp_form_submit_i` valeur `Envoyer`
  - point critique :
    - sur cette offre, ce bouton soumet directement la candidature
    - il n’existe pas de page de relecture séparée

- `success`
  - URL type :
    - `/job?mode=submit_apply&from=assessment`
    - inner frame : `/job?mode=submit_apply&from=assessment&in_iframe=1`
  - message métier confirmé :
    - `Votre candidature a bien été transmise. Merci d'avoir postulé.`
  - fallback complémentaire :
    - `Votre candidature pour ce poste a été transmise`

## Login à privilégier

Ne pas repartir du formulaire email iCIMS avec captcha observé sur `/jobs/<jobId>/login?in_iframe=1`.

Route robuste confirmée :

- `https://careers-fr-axa.icims.com/jobs/<jobId>/login?loginOnly=1&in_iframe=1`

Avantages :

- contourne le formulaire email + hCaptcha
- mène directement au flow `identifier -> password` d’iCIMS
- compatible avec l’email et le mot de passe stockés dans `career_connections/axa`

## Step 1 — Profil du candidat

### Documents

- CV
  - flag : `input#PortalProfileFields.Resume`
  - nom : `input#PortalProfileFields.Resume_FileName`
  - input réel : `input#PortalProfileFields.Resume_File[type="file"]`
  - bouton visible : `button#PortalProfileFields.Resume_Button`

- Document supplémentaire / lettre de motivation
  - titre : `input[id$="PersonProfileFields.rcf2051"]`
  - flag : `input[id$="PersonProfileFields.rcf2052"]`
  - nom fichier : `input[id$="PersonProfileFields.rcf2052_FileName"]`
  - input réel : `input[id$="PersonProfileFields.rcf2052_File"][type="file"]`
  - bouton visible : `button[id$="PersonProfileFields.rcf2052_Button"]`

Logique Taleos attendue :

- toujours recharger le CV Firebase
- toujours recharger la lettre de motivation Firebase
- titre du document additionnel : `Lettre de motivation`

### Civilité

- champ logique : `select#rcf2082`
- dropdown iCIMS visible : `a#rcf2082_icimsDropdown`
- options confirmées :
  - `Mme / Mlle`
  - `M.`
  - `Autre`

Mapping Thibault :

- `Monsieur` -> `M.`

### Identité / email

- prénom : `input#PersonProfileFields.FirstName`
- nom : `input#PersonProfileFields.LastName`
- nom d’usage : `input#rcf2010`
- email : `input#PersonProfileFields.Email`
- confirmation email / login : `input#PersonProfileFields.Login`

### Adresse

Point clé : les ids de collection deviennent dynamiques après sauvegarde.

Exemples observés :

- avant sauvegarde :
  - `-1_PersonProfileFields.AddressCountry`
  - `-1_PersonProfileFields.AddressState`
  - `-1_PersonProfileFields.AddressCity`
  - `-1_PersonProfileFields.AddressZip`
- après sauvegarde :
  - `618035_PersonProfileFields.AddressCountry`
  - `618035_PersonProfileFields.AddressState`
  - `618035_PersonProfileFields.AddressCity`
  - `618035_PersonProfileFields.AddressZip`

Le filler doit donc cibler par suffixes :

- `select[id$="PersonProfileFields.AddressCountry"]`
- `select[id$="PersonProfileFields.AddressState"]`
- `input[id$="PersonProfileFields.AddressCity"]`
- `input[id$="PersonProfileFields.AddressZip"]`

Dropdowns iCIMS à recherche interne :

- pays :
  - trigger : `a[id$="PersonProfileFields.AddressCountry_icimsDropdown"]`
  - recherche : `div[id$="PersonProfileFields.AddressCountry_icimsDropdown_ctnr"] input.dropdown-search`
  - liste : `ul[id$="PersonProfileFields.AddressCountry_dropdown-results"]`
- département / état :
  - trigger : `a[id$="PersonProfileFields.AddressState_icimsDropdown"]`
  - recherche : `div[id$="PersonProfileFields.AddressState_icimsDropdown_ctnr"] input.dropdown-search`
  - liste : `ul[id$="PersonProfileFields.AddressState_dropdown-results"]`

Comportement confirmé :

- le `fill()` simple ne suffit pas toujours pour filtrer
- `pressSequentially()` dans la search box fonctionne
- exemple validé en live :
  - `France`
  - `Val-d'Oise`

Valeurs confirmées pour Thibault :

- pays : `France`
- département / état : `Val-d'Oise`
- ville : `Sannois`
- code postal : `95110`

Valeur interne observée pour `Val-d'Oise` :

- `13176`

### Téléphone

- champ : `input[id$="PersonProfileFields.PhoneNumber"]`
- libellé AXA :
  - `Veuillez saisir votre numéro de téléphone selon le format suivant, y compris le code pays : +XX XXX XXX XXX`

Valeur validée en live :

- `+33 758953565`

### Pays du poste

- champ : `select#PersonProfileFields.RegulatoryCountry`
- valeur attendue pour cette offre :
  - `France`

### Origine de candidature

- champ : `select#rcf3048`
- options observées :
  - `Nurture CRM`
  - `AXA CAREER SITE`
  - `HELLOWORK`
  - `INDEED`
  - `JOBTEASER`
  - `LINKEDIN`
  - `OTHER`
  - `REFERRAL`

Valeur Taleos attendue :

- `AXA CAREER SITE`

### Champ conditionnel complémentaire

- texte : `input#rcf3049_Text`
- select : `select#rcf3049`
- fallback confirmé :
  - `Non applicable`

### Consentement IA / traitement

- champ : `select#rcf3339`
- options observées :
  - `Accepter`
  - `Refuser`

Valeur observée par défaut sur cette offre :

- `Accepter`

## Step 2 — Questions supplémentaires

Champs confirmés :

- `input#Q389`
  - question :
    - `Are You A Current Employee of AXA? ... If No, please enter "No" ...`
  - mapping Thibault :
    - `No`

- `input#Q383`
  - question :
    - `Are you currently or have you in the past been a partner or audit staff member of any Ernst & Young ("E&Y") firm? ...`
  - mapping Thibault :
    - `No`

- `select#Q388`
  - question :
    - `What Is Your Notice Period`
  - options observées :
    - `2 Weeks`
    - `1 Month`
    - `2 Months`
    - `3 Months`
    - `4+ Months`
    - `Available Immediately`
  - mapping Taleos :
    - `sg_notice_period = none` -> `Available Immediately`
    - `1_month` -> `1 Month`
    - `2_months` -> `2 Months`
    - `3_months` -> `3 Months`
    - `more_than_3_months` -> `4+ Months`

- `input#Q181`
  - question :
    - `What are your salary expectations?`
  - point de vigilance :
    - aucune donnée Firebase normalisée observée aujourd’hui pour ce champ
    - nécessite une réponse explicite côté profil Taleos ou une revue manuelle avant le futur clic final automatique

## Step 3 — Évaluations

Sur l’offre `4271`, aucun écran exploitable n’a été rencontré.

Comportement réel observé :

- le bouton `Envoyer` du Step 2 mène directement à la confirmation finale
- l’URL de succès contient `from=assessment`, mais aucun formulaire Step 3 n’a été affiché

Conclusion :

- le filler AXA doit considérer que certaines offres sautent directement du Step 2 au succès

## Données Firebase utilisées

- identité :
  - `civility`
  - `firstname`
  - `lastname`
  - `email`
- téléphone :
  - `phone_country_code`
  - `phone-number`
- adresse :
  - `address`
  - `zipcode`
  - `city`
  - `country`
- documents :
  - `cv_storage_path`, `cv_filename`
  - `lm_storage_path`, `lm_filename`
- AXA :
  - `career_connections.axa.email`
  - `career_connections.axa.password`
  - `axa_talent_pool`
- préférences transverses :
  - `sg_notice_period`

## Talent pool AXA

Question normalisée côté profil Taleos dans `Alertes et viviers talents`.

Correspondance métier AXA observée avant login iCIMS classique :

- `gdpr_consent_type = 37002057001`
  - rejoindre la communauté AXA / conserver le CV pour d’autres opportunités
- `gdpr_consent_type = 37002057002`
  - uniquement pour cette candidature

Même si le flow retenu pour Taleos passe par `loginOnly=1`, cette préférence reste utile à mapper pour les variantes AXA qui repasseraient par l’écran email iCIMS.

## Message de confirmation capturé

Message métier principal :

- `Votre candidature a bien été transmise. Merci d'avoir postulé.`

Texte complémentaire utile :

- `Votre candidature pour ce poste a été transmise`

## Logs attendus

Style Crédit Agricole :

- `🧾 AXA → audit détaillé Firebase vs formulaire (step 1)`
- `✅ <champ> : formulaire='...' | Firebase='...' -> Skip`
- `✏️ <champ> : formulaire='...' | Firebase='...' -> Correction`
- `✅ CV : <filename> (Firebase)`
- `✅ Lettre de motivation : <filename> (Firebase)`
- `🧾 AXA → audit détaillé Firebase vs formulaire (step 2)`
- `✏️ Notice period : formulaire='...' | Firebase='Available Immediately' -> Correction`
- `⚠️ Salary expectations : aucune donnée Taleos -> revue manuelle`
- `✅ AXA : succès confirmé -> Votre candidature a bien été transmise. Merci d'avoir postulé.`
