# Guide des commandes Terraform — Projet SNCF Streaming Pipeline

Référence des commandes Terraform utilisées (ou utiles) dans ce projet,
organisées par ordre d'utilisation réelle dans un cycle de travail normal.

---

## 1. Cycle de vie principal

### `terraform init`
**Utilité** : initialise le dossier de travail -- télécharge le provider
(`azurerm` dans notre cas), configure le backend de state, prépare les
modules. **À exécuter une seule fois** par dossier, puis à nouveau
seulement si on ajoute un nouveau provider ou change de backend.
**Dans ce projet** : exécuté une fois au tout début, a créé
`.terraform.lock.hcl` (versions exactes du provider, à committer) et le
dossier `.terraform/` (binaires du provider, jamais committé --
volontairement exclu par `.gitignore` après l'incident du 18/08).

### `terraform validate`
**Utilité** : vérifie que la syntaxe HCL est correcte et que les
références entre ressources sont cohérentes (ex. `azurerm_key_vault.sncf.id`
existe bien). **Ne contacte pas Azure** -- vérification purement locale,
donc rapide et gratuite. Toujours exécutée avant `plan` dans ce projet, pour
détecter les erreurs de syntaxe avant même d'interroger Azure.

### `terraform plan`
**Utilité** : calcule et affiche ce qui *serait* fait (créer/modifier/
détruire) si on appliquait la configuration -- sans jamais rien changer
réellement. **Toujours gratuit**, quel que soit le nombre de ressources
simulées. C'est la commande principale utilisée dans ce projet jusqu'ici --
tous les modules (Resource Group, réseau, Key Vault, ADLS Gen2, Event Hubs,
Databricks) ont été validés uniquement en `plan`, jamais appliqués.

### `terraform apply`
**Utilité** : crée/modifie réellement les ressources dans Azure, selon le
plan calculé. **Facturation immédiate** dès la création (Databricks et AKS
en particulier). **Jamais exécutée dans ce projet à ce stade** -- décision
volontaire, budget étudiant limité (voir
notes/decisions_architecture.md).

### `terraform destroy`
**Utilité** : supprime toutes les ressources précédemment créées par
`apply`. Stratégie retenue pour ce projet, si déploiement réel un jour :
`apply` → démonstration/captures d'écran → `destroy` dans la même session,
pour limiter l'exposition à quelques heures de facturation plutôt que des
jours entiers si on oublie.

---

## 2. Inspection (utiles après un `apply`, jamais encore utilisées ici)

### `terraform show`
Affiche l'état actuel des ressources réellement déployées (lecture du
fichier de state).

### `terraform state list`
Liste tous les identifiants de ressources gérées par Terraform (ex.
`azurerm_resource_group.sncf`).

### `terraform output`
Affiche les valeurs des `output` définis (ex. `storage_account_name`,
`vnet_name`) -- utile pour récupérer une information sans fouiller dans le
portail Azure.

---

## 3. Bonnes pratiques complémentaires

### `terraform fmt`
Reformate automatiquement les fichiers `.tf` selon le style standard
(indentation, alignement) -- purement cosmétique, aucun risque.

### `terraform plan -out=plan.tfplan`
Sauvegarde le plan calculé dans un fichier, pour garantir que l'`apply`
suivant exécute *exactement* ce qui a été vérifié (sans risque qu'un
changement externe à Azure, survenu entre les deux commandes, modifie le
résultat). Non utilisé ici puisqu'aucun `apply` n'a encore eu lieu.

### `terraform workspace`
Permet de gérer plusieurs environnements (dev/prod) avec la même
configuration mais des states séparés. Non utilisé dans ce projet -- la
distinction dev/prod est gérée plus simplement via la variable
`var.environment` (voir `variables.tf`), suffisant pour un projet de cette
taille.

---

## Résumé -- ce qui a été réellement exécuté dans ce projet

| Commande | Utilisée ? | Fréquence |
|---|---|---|
| `terraform init` | ✅ | 1 fois |
| `terraform validate` | ✅ | Avant chaque `plan` |
| `terraform plan` | ✅ | À chaque nouveau module (7 fois à ce jour) |
| `terraform apply` | ❌ | Jamais -- décision volontaire |
| `terraform destroy` | ❌ | Jamais -- rien à détruire |
