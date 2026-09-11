# Incidents 2026-09-10 — Démarrage Phase 2 (déploiement Azure réel)

## 1. Event Hubs `sku = "Basic"` — protocole Kafka totalement indisponible

**Symptôme anticipé avant même de tester** : lecture du module Terraform
existant (`eventhubs.tf`, issu du cycle de test du 19/08) montrant
`sku = "Basic"`.

**Cause** : Basic ne supporte pas du tout le protocole Kafka (pas une
question de perf/coût comme le suggérait le commentaire d'origine — limite
dure d'Azure). Seuls Standard, Premium et Dedicated exposent l'endpoint
Kafka. `spark-sql-kafka-0-10` n'aurait jamais pu se connecter.

**Fix** : `sku = "Basic"` → `sku = "Standard"`. Changement in-place confirmé
par `terraform plan` (pas de destroy/recreate).

---

## 2. Premier `terraform apply` réel — 30 ressources, 3 blocages en cascade

**Contexte** : le state Terraform était vide (dernier `destroy` le 19/08).
`terraform plan` sans `-target` a révélé que la quasi-totalité de
l'infra cible (30 ressources : AKS, Databricks, Event Hubs, Storage/ADLS
Gen2 + 3 containers, Key Vault, VNet + 4 subnets, Grafana, Monitor
Workspace, role assignments) restait à créer, pas juste Event Hubs.

**Blocage 1** : `MissingSubscriptionRegistration` sur `Microsoft.Monitor` —
abonnement jamais utilisé auparavant, resource provider jamais enregistré.
Fix : `az provider register --namespace Microsoft.Monitor` (+ `Microsoft.Dashboard`
en anticipation, également nécessaire pour Grafana).

**Blocage 2** : `SkuIsNotSupported` sur Grafana — `sku = "Essential"`
(présent dans le `.tf` avec un warning de dépréciation déjà visible dans
chaque plan précédent) rejeté par l'API avec `400 Bad Request`. Fix :
`sku = "Standard"`.

**Blocage 3**, en cascade du précédent : `GrafanaMajorVersionNotSupported`
— `grafana_major_version = "11"` incompatible avec le sku Standard
(versions valides : 12, 13). Fix : `"12"`.

**Résultat final** : `Apply complete! Resources: 30 added`. AKS et
Databricks (les deux ressources qui facturent en continu peu importe
l'usage) confirmés créés dès la première vague, avant même les 3 blocages
ci-dessus (résolus en parallèle sur les ressources indépendantes).

**Clarification retenue** : local et Azure sont deux environnements
totalement indépendants. Le déploiement Azure ne modifie ni n'arrête rien
en local — la bascule entre les deux est un choix de configuration
applicative (`.env`), jamais une conséquence automatique de l'infra créée.

---

## 3. Règle Event Hubs `listen` ajoutée

Ajout de `azurerm_eventhub_authorization_rule.sncf_raw_listen` (symétrique
à la règle `send` existante, principe de moindre privilège) +
`azurerm_key_vault_secret.eventhub_listen_connection_string`. Appliqué
proprement via `-target` (2 ajoutés, AKS non touché malgré une dérive
Terraform cosmétique détectée en parallèle sur `upgrade_settings` — voir
dette technique).

---

## 4. Erreur de conception sur l'accès Key Vault — non résolue ce jour

Tentative d'ajouter l'accès personnel de Franck à Key Vault en modifiant
**directement** la policy existante `terraform_sp` (remplacement de
`object_id` par l'identité personnelle) plutôt que d'ajouter une policy
séparée. Un `terraform plan` a montré un `object_id` inchangé par
coïncidence à ce stade (résolution incorrecte de la situation ce jour-là
— voir `incidents_2026-09-11.md` pour la correction complète et la cause
réelle, découverte le lendemain via un `403 Forbidden`).
