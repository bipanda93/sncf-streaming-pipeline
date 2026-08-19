# Journal des incidents — 19 août 2026

Session de travail : premier déploiement réel de l'infrastructure Terraform
(apply -> vérification -> destroy), suite logique de la construction des 8
modules la veille.

**Bilan** : 6 erreurs réelles rencontrées lors du apply, chacune diagnostiquée
et corrigée avant de relancer -- aucune correction à l'aveugle.

## Résumé des 6 corrections

| # | Erreur | Cause | Correction |
|---|---|---|---|
| 1 | `DatabricksStandardSkuNotSupported` | SKU Standard retiré par Azure | `sku = "premium"` |
| 2 | VM Standard_D2s_v3 non autorisée | Hors liste des tailles du compte étudiant | Tentative Standard_D2s_v5 |
| 3 | `ServiceCidrOverlapExistingSubnetsCidr` | Plage réseau AKS par défaut chevauchait le VNet (10.0.0.0/16) | `service_cidr = "172.16.0.0/16"` explicite |
| 4 | `storage_account_identity` vide (IAM) | Mauvais attribut -- concerne le stockage interne DBFS, pas un accès externe | `azurerm_databricks_access_connector` dédié |
| 5 | `AuthorizationFailed` sur les role assignments | Le Service Principal (rôle Contributor) n'avait pas le droit d'attribuer des rôles à d'autres identités | `az role assignment create --role "User Access Administrator"` sur le SP, depuis le compte utilisateur |
| 6 | `InsufficientVCPUQuota` (D2s_v5, puis B2s_v2) | Quota vCPU à 0 pour ces familles sur le compte étudiant | `az vm list-usage` pour croiser tailles autorisées ET quota disponible -> Standard_D2s_v6 |

**Résultat final** : apply réussi (26 ressources), vérifié visuellement dans
le portail Azure (captures pour le mémoire), puis destroy complet (27
ressources supprimées) pour arrêter toute facturation -- cycle complet en
une seule session, conformément à la politique retenue dans
notes/decisions_architecture.md.

## Enseignement méthodologique

L'erreur 6 illustre l'intérêt de croiser deux informations distinctes
(tailles de VM autorisées + quota disponible par famille) plutôt que de
corriger un seul symptôme à la fois -- les tentatives 2 et "B2s_v2"
corrigeaient chacune un aspect (taille autorisée) sans vérifier l'autre
(quota réellement disponible), d'où deux échecs supplémentaires avant la
bonne combinaison.
