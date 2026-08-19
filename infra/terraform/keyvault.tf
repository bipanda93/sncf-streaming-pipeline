# Key Vault -- stocke les futurs secrets du projet (chaîne de connexion
# ADLS Gen2, identifiants Event Hubs, token Claude API...), plutôt que de
# les laisser dans des fichiers .env locaux comme aujourd'hui.

data "azurerm_client_config" "current" {}

resource "azurerm_key_vault" "sncf" {
  name                = "kv-sncf-${var.environment}"
  resource_group_name = azurerm_resource_group.sncf.name
  location            = azurerm_resource_group.sncf.location
  tenant_id           = data.azurerm_client_config.current.tenant_id
  sku_name            = "standard"

  # Désactivé en dev : la protection contre la purge empêcherait une
  # suppression définitive pendant 90 jours après un "terraform destroy" --
  # gênant pour un environnement de test/portfolio, à réactiver en prod.
  purge_protection_enabled = false

  tags = local.common_tags
}

# Autorise le Service Principal utilisé par Terraform à gérer les secrets --
# sans ça, Terraform pourrait créer le coffre mais jamais y écrire quoi que
# ce soit.
resource "azurerm_key_vault_access_policy" "terraform_sp" {
  key_vault_id = azurerm_key_vault.sncf.id
  tenant_id    = data.azurerm_client_config.current.tenant_id
  object_id    = data.azurerm_client_config.current.object_id

  secret_permissions = ["Get", "List", "Set", "Delete", "Purge"]
}
