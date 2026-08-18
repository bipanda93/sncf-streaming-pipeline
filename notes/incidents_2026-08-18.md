
## 6. Secret Azure commité par erreur — .gitignore mal placé

| | |
|---|---|
| **Problème** | `.env.terraform` (contenant `ARM_CLIENT_SECRET`, un secret Service Principal avec rôle Contributor sur tout l'abonnement Azure) committé en clair dans l'historique Git (commit b4d355e) |
| **Cause** | `.gitignore` créé depuis l'intérieur de `infra/terraform/` avec un motif `infra/terraform/.env.terraform` -- chemin doublement imbriqué, ne correspondant à aucun fichier réel, donc sans effet -- même famille de bug que l'erreur de chemin relatif rencontrée plus tôt dans la session (point 3) |
| **Détection** | Repérée en relisant attentivement la liste des fichiers avant tout commit, discipline déjà appliquée tout au long du projet |
| **Remédiation, dans l'ordre** | (1) Révocation immédiate de l'ancien secret via `az ad sp credential reset` -- neutralise le risque réel, indépendamment de l'état de Git ; (2) `git reset --soft HEAD~1` pour annuler le commit fautif (jamais poussé vers un remote, donc récupérable simplement) ; (3) `.gitignore` corrigé avec un chemin relatif correct ; (4) nouveau secret inséré ; (5) vérification explicite de `git status` avant tout nouveau commit |
| **Validation** | Nouveau commit propre, aucun secret ; ancien secret confirmé invalide |
