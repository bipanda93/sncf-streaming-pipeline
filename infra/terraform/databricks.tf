# Databricks Workspace -- exécute les jobs Spark (Bronze/Silver/Gold) en
# remplacement des scripts locaux. Inclut nativement Unity Catalog (déjà
# présent comme dépendance dans tous les runs Spark du projet depuis le
# début, jamais configuré jusqu'ici) et MLflow (suivi du futur modèle
# Isolation Forest).

resource "azurerm_databricks_workspace" "sncf" {
  name                = "dbw-sncf-${var.environment}"
  resource_group_name = azurerm_resource_group.sncf.name
  location            = azurerm_resource_group.sncf.location

  # Standard -- suffisant pour ce projet (pas besoin des fonctionnalités
  # Premium comme le contrôle d'accès fin par table, qu'Unity Catalog
  # apporte déjà indépendamment du SKU workspace).
  sku = "premium"

  # Injection dans notre VNet plutôt que dans un réseau managé par Azure --
  # permet une connectivité privée avec ADLS Gen2 et Event Hubs, cohérent
  # avec la décision prise pour le module réseau (voir
  # notes/decisions_architecture.md).
  custom_parameters {
    virtual_network_id                                   = azurerm_virtual_network.sncf.id
    public_subnet_name                                   = azurerm_subnet.databricks_public.name
    private_subnet_name                                  = azurerm_subnet.databricks_private.name
    public_subnet_network_security_group_association_id  = azurerm_subnet_network_security_group_association.databricks_public.id
    private_subnet_network_security_group_association_id = azurerm_subnet_network_security_group_association.databricks_private.id
  }

  tags = local.common_tags
}
