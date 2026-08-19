# AKS -- héberge Airflow en production.
#
# network_profile.service_cidr : plage dédiée aux services Kubernetes
# internes (ClusterIP...), DOIT être différente de la plage du VNet
# (10.0.0.0/16, voir network.tf) -- sans cette précision, Azure tentait par
# défaut d'utiliser une plage qui chevauchait notre VNet, provoquant une
# erreur ServiceCidrOverlapExistingSubnetsCidr. 172.16.0.0/16 n'a aucune
# intersection avec notre réseau existant.

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

  tags = local.common_tags
}
