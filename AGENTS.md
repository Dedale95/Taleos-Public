# Instructions pour l’agent (Cursor / IA)

## Git : committer et pousser TOUS les changements, à chaque fois

**Règle obligatoire** : Tu dois committer sur GitHub **tous** les changements que nous apportons, et **à chaque fois** que tu modifies des fichiers — pas seulement quand on te le demande explicitement.

- **Pourquoi** : Si tu oublies de commit, les modifications restent en local. L’utilisateur teste ensuite sur le site (déployé depuis GitHub) : ça ne fonctionne pas, alors que le problème vient simplement du fait que les changements n’ont pas été commités et poussés.
- **Quand** : Dès qu’une modification de code est terminée (ou à la fin d’une série de modifications liées), enchaîne immédiatement : `git add` (tous les fichiers concernés) → `git commit` → `git push origin <branche>`.
- **Ne jamais** s’arrêter au commit local seul : toujours pousser vers GitHub tout de suite après le commit.

En résumé : **chaque changement = commit + push**, sans exception.
