# Event Hubs -- remplace Kafka (local, Docker) par son équivalent managé
# Azure, compatible avec le protocole Kafka (les producteurs/consommateurs
# du projet n'ont besoin que de changer leur config de connexion, pas leur
# code).

resource "azurerm_eventhub_namespace" "sncf" {
  name                = "evhns-sncf-${var.environment}"
  location            = azurerm_resource_group.sncf.location
  resource_group_name = azurerm_resource_group.sncf.name

  # Basic -- suffisant pour ce projet (pas besoin de Capture, de réplication
  # géographique, ni de rétention étendue proposées par Standard/Premium).
  # Coût le plus faible des trois paliers.
  sku      = "Basic"
  capacity = 1

  tags = local.common_tags
}

# Un seul Event Hub -- équivalent du topic Kafka "sncf-raw" déjà utilisé
# en local (voir src/ingestion/config.py, TOPIC_RAW).
resource "azurerm_eventhub" "sncf_raw" {
  name         = "sncf-raw"
  namespace_id = azurerm_eventhub_namespace.sncf.id

  # 2 partitions -- suffisant pour le volume actuel du projet (quelques
  # centaines à quelques milliers de perturbations/jour). Le palier Basic
  # limite de toute façon la rétention à 1 jour, contrairement à
  # Standard/Premium qui permettraient plusieurs jours.
  partition_count   = 2
  message_retention = 1
}

# Règle d'accès dédiée à l'ingestion (écriture uniquement) -- séparée d'une
# éventuelle règle de lecture future, principe de moindre privilège.
resource "azurerm_eventhub_authorization_rule" "sncf_raw_send" {
  name                = "sncf-raw-send"
  namespace_name      = azurerm_eventhub_namespace.sncf.name
  eventhub_name       = azurerm_eventhub.sncf_raw.name
  resource_group_name = azurerm_resource_group.sncf.name

  listen = false
  send   = true
  manage = false
}

# La chaîne de connexion Event Hubs part dans Key Vault, comme pour ADLS
# Gen2 -- jamais dans un fichier local.
resource "azurerm_key_vault_secret" "eventhub_connection_string" {
  name         = "eventhub-connection-string"
  value        = azurerm_eventhub_authorization_rule.sncf_raw_send.primary_connection_string
  key_vault_id = azurerm_key_vault.sncf.id

  depends_on = [azurerm_key_vault_access_policy.terraform_sp]
}
