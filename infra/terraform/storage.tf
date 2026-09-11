# ADLS Gen2 -- remplace /tmp/delta (stockage local) par du stockage Azure
# réel, avec hiérarchie de dossiers (is_hns_enabled), requis pour Delta
# Lake en production.

resource "azurerm_storage_account" "sncf" {
  name                     = "stsncf${var.environment}"
  resource_group_name      = azurerm_resource_group.sncf.name
  location                 = azurerm_resource_group.sncf.location
  account_tier             = "Standard"
  account_replication_type = "LRS"

  # Active le "Hierarchical Namespace" -- transforme un compte de stockage
  # classique en véritable Data Lake Gen2, avec de vrais dossiers (pas de
  # simulation via préfixes comme sur un stockage objet plat). Requis pour
  # que Delta Lake fonctionne correctement (transactions ACID, fichiers
  # _delta_log).
  is_hns_enabled = true

  tags = local.common_tags
}

# Un conteneur par couche medallion -- reflète directement la structure
# /tmp/delta/{bronze,silver,gold} déjà utilisée en local, migration du
# code minimale (juste changer le chemin de base).
resource "azurerm_storage_container" "bronze" {
  name                  = "bronze"
  storage_account_id    = azurerm_storage_account.sncf.id
  container_access_type = "private"
}

resource "azurerm_storage_container" "silver" {
  name                  = "silver"
  storage_account_id    = azurerm_storage_account.sncf.id
  container_access_type = "private"
}

resource "azurerm_storage_container" "gold" {
  name                  = "gold"
  storage_account_id    = azurerm_storage_account.sncf.id
  container_access_type = "private"
}

# La clé d'accès au compte de stockage est stockée dans Key Vault plutôt
# que dans un fichier .env -- cohérent avec la remédiation appliquée après
# l'incident de sécurité du 18/08.
resource "azurerm_key_vault_secret" "storage_connection_string" {
  name         = "storage-connection-string"
  value        = azurerm_storage_account.sncf.primary_connection_string
  key_vault_id = azurerm_key_vault.sncf.id

  depends_on = [azurerm_key_vault_access_policy.terraform_sp]
}

resource "azurerm_key_vault_secret" "storage_account_key" {
  name         = "storage-account-key"
  value        = azurerm_storage_account.sncf.primary_access_key
  key_vault_id = azurerm_key_vault.sncf.id

  depends_on = [azurerm_key_vault_access_policy.terraform_sp]
}