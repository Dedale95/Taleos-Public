# Diagnostic : les données sont-elles bien sauvegardées dans Firebase ?

## Oui, le code envoie bien à Firestore

L’app écrit dans **Firestore** aux endroits suivants :

| Fonctionnalité        | Chemin Firestore                                           |
|-----------------------|------------------------------------------------------------|
| Recherche avancée     | `profiles / {votre userId} / advanced_searches / {id}`     |
| Filtres page Offres   | `profiles / {votre userId} / user_preferences / offres_filters` |
| Candidatures          | `profiles / {votre userId} / job_applications / {id}`      |
| Connexions bancaires  | `profiles / {votre userId} / career_connections / {id}`    |

- **Projet Firebase** : `project-taleos`
- **Base** : **Firestore** (pas Realtime Database)

Si vous ne voyez rien, le blocage vient en général de la **config Firebase** ou des **règles**.

---

## 1. Firestore est-il bien créé ?

1. [Firebase Console](https://console.firebase.google.com/) → projet **project-taleos**
2. Menu **Build** → **Firestore Database**
3. Si vous voyez **« Créer une base de données »** :
   - Cliquez dessus
   - Choisir **« Démarrer en mode production »** (on ajoute les règles après)
   - Région : **europe-west** (ou proche)
   - Valider

Si Firestore n’est pas créé, aucune donnée ne peut être stockée.

---

## 2. Règles Firestore pour `advanced_searches` et `user_preferences`

Sans règles adaptées, les écritures sont refusées (erreur « permissions »).

1. **Firestore Database** → onglet **Règles**
2. À l’intérieur de `match /profiles/{userId} { ... }`, vous devez avoir :

```
match /advanced_searches/{searchId} {
  allow read, create, update, delete: if request.auth != null
                                     && request.auth.uid == userId;
}

match /user_preferences/{prefId} {
  allow read, create, update, delete: if request.auth != null
                                     && request.auth.uid == userId;
}
```

3. Règles complètes de référence : **`firestore_rules_complete.txt`**
4. Cliquer sur **Publier**

---

## 3. Où regarder les données dans la console

1. **Firestore Database** → onglet **Data** (pas **Realtime Database**)
2. Structure :
   ```
   profiles (collection)
     └── {votre userId} (document)   ← identifiant Firebase Auth
           └── advanced_searches (sous-collection)
                 └── {id accordéon} (document)
   ```

Pour avoir votre `userId` :

- Console du navigateur (F12) : au chargement de la page Recherche avancée, chercher un log du type  
  `🔄 onAuthStateChanged déclenché, user: xxxxx`  
  ou  
  `🔄 [loadAccordions] Chargement depuis Firestore pour user: xxxxx`
- Ou : **Authentication** → onglet **Users** → colonne **User UID**

---

## 4. Vérifier que vous êtes bien connecté

- Les écritures ne se font **que si** `request.auth != null`, donc **utilisateur connecté**.
- Sur la page **Recherche avancée** : si vous voyez un lien type « Inscription / Connexion » en haut, vous n’êtes pas connecté → rien ne sera sauvegardé pour les accordéons.

---

## 5. Vérifier dans la console du navigateur (F12)

Sur **Recherche avancée**, après avoir cliqué sur **« Nouvelle recherche »** :

- En cas de **succès** :
  - `✅ [saveAccordion] Nouvel accordéon créé avec ID: xxxxx`
- En cas d’**erreur** :
  - `❌ [saveAccordion] Erreur lors de la sauvegarde:`  
    → regarder `code` et `message` (ex. `permission-denied` = règles ou pas connecté).

---

## 6. Checklist rapide

| Étape                                   | À faire |
|----------------------------------------|---------|
| Firestore créé dans le projet          | Firestore Database → créer la base si besoin |
| Règles avec `advanced_searches`        | Règles → coller celles de `firestore_rules_complete.txt` → **Publier** |
| Règles avec `user_preferences`         | Idem (déjà dans `firestore_rules_complete.txt`) |
| Utilisateur connecté                   | Se connecter avant de créer des accordéons |
| Regarder au bon endroit                | Firestore **Data** → `profiles` → `{userId}` → `advanced_searches` |

---

## 7. Hiérarchie Firestore : la sous-collection n'apparaît qu'après le 1er doc

Firestore **ne crée pas une sous-collection vide**. `advanced_searches` n'apparaît dans **Data** que lorsqu'**au moins un document** a été écrit avec succès (premier `add()` réussi). Si `add()` échoue (règles, réseau…), la sous-collection n'est jamais créée.

## 8. Si vous avez déjà tout fait et voyez toujours « rien »

- Vérifier le **bon projet** Firebase (en haut à gauche : **project-taleos**).
- S’assurer d’être dans **Firestore Database** > **Data**, pas dans **Realtime Database**.
- Après modification des règles : **Publier** et attendre quelques secondes, puis réessayer de créer un accordéon.
- Une fois un accordéon créé, faire **F5** sur la page Data pour voir la sous-collection `advanced_searches` et les nouveaux documents.
- Si la création échoue (permission-denied, etc.), l'accordéon est **retiré de l'affichage** (il n'existe pas dans Firebase).

---

En résumé : **l’app envoie bien les données à Firestore**. Si rien n’apparaît, c’est en général :  
1) Firestore non créé,  
2) Règles non publiées (ou sans `advanced_searches` / `user_preferences`),  
3) Utilisateur non connecté.

En suivant les étapes ci-dessus, les sauvegardes devraient apparaître sous  
`profiles / {userId} / advanced_searches`.
