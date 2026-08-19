# AKS -- héberge Airflow en production.
#
# vm_size = Standard_D2s_v6 : seule famille avec du quota vCPU disponible
# sur ce compte étudiant, trouvée par élimination le 19/08 (voir
# notes/incidents_2026-08-19.md) -- Standard_D2s_v3 (hors liste autorisée),
# Standard_D2s_v5 et Standard_B2s_v2 (quota 0) ont tous échoué avant.
#
# network_profile.service_cidr : plage dédiée aux services Kubernetes
# internes, distincte du VNet (10.0.0.0/16) pour éviter tout chevauchement.
#
# monitor_metrics : active la collecte Prometheus managée (voir
# monitoring.tf). Point de moindre certitude sur le lien exact
# AKS<->Monitor Workspace au moment de l'écriture -- à vérifier en premier
# si validate/plan échoue sur ce bloc précis.

resource "azurerm_kubernetes_cluster" "sncf" {
  name                = "aks-sncf-${var.environment}"
  location            = azurerm_resource_group.sncf.location
  resource_group_name = azurerm_resource_group.sncf.name
  dns_prefix          = "aks-sncf-${var.environment}"

  sku_tier = "Free"

  default_node_pool {
    name           = "default"
    node_count     = 1
    vm_size        = "Standard_D2s_v6"
    vnet_subnet_id = azurerm_subnet.aks.id
  }

  identity {
    type = "SystemAssigned"
  }

  network_profile {
    network_plugin = "kubenet"
    service_cidr   = "172.16.0.0/16"
    dns_service_ip = "172.16.0.10"
  }

  monitor_metrics {}

  tags = local.common_tags
}
