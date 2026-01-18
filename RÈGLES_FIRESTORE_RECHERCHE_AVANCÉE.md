# 🔧 Corriger l’erreur "Erreur de permissions" – Recherche avancée

## Problème

Le message **« Erreur de permissions. Vérifiez que vous êtes connecté et que les règles Firestore permettent l'écriture. »** apparaît car les **règles Firestore** n’autorisent pas l’écriture dans :

- `profiles/{userId}/advanced_searches` → **recherche avancée** (accordéons)
- `profiles/{userId}/user_preferences` → **filtres de la page Offres**

Sans ces règles, les accordéons ne sont **jamais sauvegardés** dans Firestore. Au changement de page, le rechargement ne trouve rien → les accordéons semblent disparaître.

---

## ✅ À faire dans Firebase

### 1. Ouvrir la console Firestore

1. Aller sur [Firebase Console](https://console.firebase.google.com/)
2. Choisir le projet (ex. `project-taleos`)
3. Menu gauche → **Firestore Database**
4. Onglet **Règles**

### 2. Remplacer les règles

Remplacer **tout** le contenu par les règles ci‑dessous (elles incluent `advanced_searches` et `user_preferences`) :

```
rules_version = '2';
service cloud.firestore {
  match /databases/{database}/documents {

    match /profiles/{userId} {
      allow read, create, update, delete: if request.auth != null
                                         && request.auth.uid == userId;

      match /job_applications/{appId} {
        allow read, create, update, delete: if request.auth != null
                                           && request.auth.uid == userId;
      }

      match /career_connections/{connectionId} {
        allow read, create, update, delete: if request.auth != null
                                           && request.auth.uid == userId;
      }

      match /advanced_searches/{searchId} {
        allow read, create, update, delete: if request.auth != null
                                           && request.auth.uid == userId;
      }

      match /user_preferences/{prefId} {
        allow read, create, update, delete: if request.auth != null
                                           && request.auth.uid == userId;
      }
    }
  }
}
```

### 3. Publier

Cliquer sur **« Publier »** en haut à droite.

---

## Vérification

1. Se **connecter** sur le site
2. Aller dans **Recherche avancée**
3. Cliquer sur **« Nouvelle recherche »** → un accordéon doit apparaître
4. **Changer de page** (ex. Offres) puis **revenir** sur Recherche avancée  
   → Les accordéons doivent **rester** (plus d’erreur de permissions, plus de disparition)

---

## Index éventuel pour le tri

Si une erreur du type **« failed-precondition »** ou **« The query requires an index »** apparaît, créer l’index comme indiqué dans le message (lien dans la console), ou :

1. Firestore Database → **Index**
2. **Créer un index**
3. Collection : `advanced_searches` (sous `profiles/{userId}`)
4. Champs : `order` (Ascending)

Le code gère déjà un fallback sans `orderBy` si l’index manque, mais avec l’index le tri sera correct.

---

## Récap

| Sous-collection     | Rôle                                      |
|---------------------|-------------------------------------------|
| `job_applications`  | Candidatures (page Offres, Mes candidatures) |
| `career_connections`| Connexions bancaires (page Connexions)    |
| `advanced_searches` | Recherches avancées (accordéons)          |
| `user_preferences`  | Filtres Offres, autres préférences        |

Toutes sont limitées à l’utilisateur connecté : `request.auth.uid == userId`.
