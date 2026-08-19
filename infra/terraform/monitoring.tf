# Monitoring -- Azure Monitor managed service for Prometheus + Azure
# Managed Grafana. Décision documentée dans notes/decisions_architecture.md
# (section "Monitoring") : facturé à l'ingestion/requête plutôt qu'à
# l'hébergement -- évite de reproduire le problème de quota vCPU rencontré
# lors du premier apply réel (voir notes/incidents_2026-08-19.md), puisque
# Prometheus ne tourne pas sur un nœud AKS dédié.

# Store des métriques Prometheus -- équivalent "métriques" d'un Log
# Analytics workspace. Azure provisionne automatiquement un Data
# Collection Endpoint et une Data Collection Rule par défaut derrière
# cette ressource.
resource "azurerm_monitor_workspace" "sncf" {
  name                = "amw-sncf-${var.environment}"
  resource_group_name = azurerm_resource_group.sncf.name
  location            = azurerm_resource_group.sncf.location

  tags = local.common_tags
}

# Grafana managé -- aucun hébergement à notre charge, aucun nœud AKS
# supplémentaire requis. Intégration directe au Monitor Workspace via
# azure_monitor_workspace_integrations (méthode actuelle, plus simple que
# l'ancienne intégration par CLI en provisioner local-exec).
resource "azurerm_dashboard_grafana" "sncf" {
  name                = "grafana-sncf-${var.environment}"
  resource_group_name = azurerm_resource_group.sncf.name
  location            = azurerm_resource_group.sncf.location

  sku = "Essential"
  grafana_major_version = 11

  api_key_enabled                   = true
  deterministic_outbound_ip_enabled = false
  public_network_access_enabled     = true

  identity {
    type = "SystemAssigned"
  }

  azure_monitor_workspace_integrations {
    resource_id = azurerm_monitor_workspace.sncf.id
  }

  tags = local.common_tags
}

# Autorise l'identité managée de Grafana à lire les métriques -- sans ce
# rôle, Grafana afficherait une source de données connectée mais vide.
resource "azurerm_role_assignment" "grafana_to_monitor_workspace" {
  scope                = azurerm_monitor_workspace.sncf.id
  role_definition_name = "Monitoring Reader"
  principal_id         = azurerm_dashboard_grafana.sncf.identity[0].principal_id
}
