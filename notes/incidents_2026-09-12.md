# Incidents 2026-09-12 — Airflow sur AKS, du zéro au pipeline complet fonctionnel

## 1. IAM AKS — identité du cluster confondue avec l'identité des nœuds

**Découverte** : en préparant le déploiement AKS, relecture d'`iam.tf`
existant. `aks_to_keyvault` et `aks_to_eventhub` utilisaient
`azurerm_kubernetes_cluster.sncf.identity[0].principal_id` — l'identité
du **plan de contrôle** du cluster (gestion interne AKS de ses propres
ressources Azure), pas l'identité des **nœuds**
(`kubelet_identity[0].object_id`), celle que les pods utilisent
réellement via `ManagedIdentityCredential`/`DefaultAzureCredential`.

**Bonus découvert au passage** : `aks_to_keyvault` était en plus un
`azurerm_role_assignment` RBAC — même piège que `franck_read` avant lui,
le coffre `kv-sncf-dev` fonctionne en mode Access Policies, pas RBAC.
Ce role assignment n'avait donc jamais eu d'effet réel, depuis sa
création.

**Fix** : `aks_to_keyvault` remplacé par une vraie
`azurerm_key_vault_access_policy` (`aks_read`, `secret_permissions =
["Get"]`) sur `kubelet_identity[0].object_id` ; `aks_to_eventhub`
recréé sur la même identité (`principal_id` immuable sur un role
assignment Azure — recréation obligatoire, pas une modification en
place) ; nouveau `aks_to_acr` (rôle `AcrPull`) ajouté sur la même base
pour le registre. `terraform apply` : 4 ajoutés, 1 changé (dérive AKS
cosmétique déjà connue), 2 détruits. Vérifié via `az keyvault show` (3
policies actives) et `az acr show`.

---

## 2. Docker login ACR — wrapper Docker `az` incompatible

**Symptôme** : `az acr login --name acrsncfdev` → `DOCKER_COMMAND_ERROR,
Please verify if Docker client is installed and running`, alors que
Docker Desktop tourne parfaitement sur la machine.

**Cause** : `az acr login` appelle `docker login` en interne — mais la
fonction `az()` de `.zshrc` exécute la commande **dans** un conteneur
Docker, qui n'a ni Docker installé ni accès au démon Docker de l'hôte.
"Docker absent" est vrai du point de vue du conteneur, pas de la vraie
machine.

**Fix, en deux temps** : `az acr login --expose-token` génère un jeton
réutilisable directement avec `docker login --password-stdin` — mais la
première tentative, capturée via la fonction `.zshrc`, contenait des
caractères `\r` invisibles injectés par le mode `-it` du conteneur
(confirmé via `tr -d '\r' | wc -c` renvoyant une longueur différente).
Résolu en utilisant le binaire natif `/opt/anaconda3/bin/az` pour cette
capture — même leçon que Key Vault il y a quelques jours : préférer le
binaire natif à la fonction Docker dès qu'une sortie doit être
réutilisée proprement, pas juste affichée à l'écran.

---

## 3. Installation Helm — même piège Homebrew que `azure-cli`

**Symptôme** : `brew install helm` tente de compiler `go` depuis les
sources (`./make.bash`), à cause des mêmes Command Line Tools
désynchronisées que lors de l'installation `azure-cli`. `--ignore-
dependencies` empire la situation (`make: go: Command not found`) au
lieu de la contourner.

**Fix** : script d'installation officiel Helm
(`get-helm-3`), binaire précompilé, aucune compilation. `helm v3.22.0`
opérationnel en quelques secondes.

---

## 4. Test IMDS — Workload Identity Federation non nécessaire

Avant d'écrire `values.yaml`, vérification empirique (pod jetable,
requête `curl` vers `169.254.169.254/metadata/identity/oauth2/token`)
que `kubelet_identity` est directement accessible depuis un pod sans
configuration Kubernetes supplémentaire. Confirmé : un `access_token`
valide est retourné. AKS expose l'IMDS legacy directement aux pods sur
ce cluster — pas besoin de fédération OIDC, de ServiceAccount annoté, ni
d'identité managée séparée. Simplifie considérablement le reste du
déploiement.

---

## 5. Choix d'architecture Helm — `KubernetesExecutor`, DAGs intégrés à l'image

Deux décisions prises avant d'écrire `values.yaml`, détaillées dans
`decisions_architecture.md`.

---

## 6. Logs Kubernetes non persistants — StatefulSet immutable, `ReadWriteMany` vs storageclass par défaut

**Symptôme initial** : premier `helm install` réussi, tous pods
`Running`, mais tâche échouée sans logs consultables (`Could not read
served logs`, pod éphémère déjà supprimé).

**Tentative 1** : `logs.persistence.enabled: true` ajouté, `helm
upgrade` → `UPGRADE FAILED: cannot patch "airflow-triggerer"... forbidden`.
Un `StatefulSet` interdit toute modification de son
`volumeClaimTemplates` après création — pas contournable par une
meilleure configuration, seule une recréation complète fonctionne.

**Tentative 2** : `helm uninstall` + `helm install` — la nouvelle PVC
`airflow-logs` reste bloquée en `Pending`. `kubectl get pvc` révèle des
PVC d'un **premier** déploiement encore présentes (56 min d'âge) :
`helm uninstall` préserve volontairement les PVC par défaut (protection
contre la perte de données), créant un conflit de nommage.

**Tentative 3** : nettoyage complet (`helm uninstall` + `kubectl delete
pvc --all`) — `airflow-logs` **toujours** `Pending`, cette fois pour une
tout autre raison. `kubectl describe pod` révèle `FailedScheduling`,
`VolumeBinding`. Cause racine : la PVC demande `accessModes:
ReadWriteMany` (plusieurs pods — scheduler, api-server, dag-processor,
triggerer — doivent lire/écrire les mêmes logs simultanément), mais
`storageClassName: default` pointe vers `disk.csi.azure.com`, qui ne
supporte que `ReadWriteOnce`.

**Fix définitif** : `storageClassName: azurefile` explicite sous
`logs.persistence` — classe déjà disponible nativement sur AKS
(`file.csi.azure.com`, supporte RWX), aucune ressource Terraform
supplémentaire nécessaire. Nettoyage complet + réinstallation : PVC
`Bound`, tous pods `Running`.

---

## 7. `cd: /opt/airflow/project: No such file or directory` — code source jamais copié dans l'image

**Symptôme** (une fois les logs enfin persistés et consultables) :
`sncf_to_kafka` échoue avec `bash: line 1: cd: /opt/airflow/project: No
such file or directory`.

**Cause** : `Dockerfile.airflow` copiait `dags/` mais jamais `src/` —
le code Python (`config.py`, `main.py`, etc.) n'a jamais existé dans
l'image. Fonctionnait en local uniquement parce que
`docker-compose.yml` monte tout le dossier projet (`./:/opt/airflow/project`),
un mécanisme qui n'existe pas sur AKS.

**Fix** : `COPY src/ /opt/airflow/project/src/` ajouté après le `COPY
dags/` existant (même logique d'ordre pour le cache Docker : après le
`pip install`, pas avant). Rebuild, push, `kubectl rollout restart` sur
scheduler et dag-processor pour forcer la récupération de la nouvelle
image (`pullPolicy: Always` ne suffit pas seul à déclencher un
redéploiement).

---

## 8. Résultat final — pipeline complet validé sur AKS

Run `scheduled__2026-09-12T14:00:00` : `sncf_to_kafka` réussi ("2
perturbation(s) publiée(s)"), puis `kafka_to_bronze` réussi (étapes
Spark visibles dans les logs persistés). Déclenché automatiquement par
la programmation horaire du DAG, sans intervention manuelle — la
première preuve que l'orchestration tourne réellement sur Kubernetes,
pas juste que l'infrastructure existe.

**Les 3 composants de la Phase 2 (Event Hubs, ADLS Gen2, AKS) sont
maintenant tous validés en conditions réelles.**
