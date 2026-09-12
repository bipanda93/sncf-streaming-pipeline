# IAM -- attribue des permissions RBAC aux identités managées de Databricks
# et AKS, plutôt que de leur donner des secrets à stocker.

# Access Connector -- identité dédiée permettant à Databricks d'accéder à
# des ressources externes (notre ADLS Gen2). Nécessaire car
# azurerm_databricks_workspace n'expose pas d'identité générale utilisable
# directement (storage_account_identity concerne uniquement le stockage
# interne DBFS, pas un accès externe) -- pattern standard Azure/Databricks,
# aussi requis pour une future intégration Unity Catalog.
resource "azurerm_databricks_access_connector" "sncf" {
  name                = "dbac-sncf-${var.environment}"
  resource_group_name = azurerm_resource_group.sncf.name
  location            = azurerm_resource_group.sncf.location

  identity {
    type = "SystemAssigned"
  }

  tags = local.common_tags
}

resource "azurerm_role_assignment" "databricks_to_storage" {
  scope                = azurerm_storage_account.sncf.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azurerm_databricks_access_connector.sncf.identity[0].principal_id
}

resource "azurerm_key_vault_access_policy" "aks_read" {
  key_vault_id = azurerm_key_vault.sncf.id
  tenant_id    = data.azurerm_client_config.current.tenant_id
  object_id    = azurerm_kubernetes_cluster.sncf.kubelet_identity[0].object_id

  secret_permissions = ["Get"]
}

resource "azurerm_role_assignment" "aks_to_eventhub" {
  scope                = azurerm_eventhub_namespace.sncf.id
  role_definition_name = "Azure Event Hubs Data Owner"
  principal_id         = azurerm_kubernetes_cluster.sncf.kubelet_identity[0].object_id
}

resource "azurerm_role_assignment" "aks_to_acr" {
  scope                            = azurerm_container_registry.sncf.id
  role_definition_name             = "AcrPull"
  principal_id                     = azurerm_kubernetes_cluster.sncf.kubelet_identity[0].object_id
  skip_service_principal_aad_check = true
}
