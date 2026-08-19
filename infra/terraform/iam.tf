# IAM -- attribue des permissions RBAC aux identités managées de Databricks
# et AKS, plutôt que de leur donner des secrets à stocker. C'est la pièce
# qui referme la boucle commencée avec Key Vault : les futurs échanges
# entre services n'auront jamais besoin d'un mot de passe circulant en
# clair, contrairement au Service Principal qu'on utilise nous-mêmes pour
# faire tourner Terraform (voir l'incident du 18/08, notes/incidents_2026-08-18.md).

# Databricks -> ADLS Gen2 : autorise le workspace à lire/écrire les 3
# conteneurs (bronze/silver/gold) via son identité managée, sans clé
# d'accès partagée.
resource "azurerm_role_assignment" "databricks_to_storage" {
  scope                = azurerm_storage_account.sncf.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azurerm_databricks_workspace.sncf.storage_account_identity[0].principal_id
}

# AKS -> Key Vault : autorise le cluster (donc, à terme, Airflow qui y
# tourne) à lire les secrets nécessaires à l'orchestration (chaînes de
# connexion Event Hubs, ADLS Gen2).
resource "azurerm_role_assignment" "aks_to_keyvault" {
  scope                = azurerm_key_vault.sncf.id
  role_definition_name = "Key Vault Secrets User"
  principal_id         = azurerm_kubernetes_cluster.sncf.identity[0].principal_id
}

# AKS -> Event Hubs : autorise le cluster à consommer/produire sur le hub
# 'sncf-raw' via son identité managée.
resource "azurerm_role_assignment" "aks_to_eventhub" {
  scope                = azurerm_eventhub_namespace.sncf.id
  role_definition_name = "Azure Event Hubs Data Owner"
  principal_id         = azurerm_kubernetes_cluster.sncf.identity[0].principal_id
}
