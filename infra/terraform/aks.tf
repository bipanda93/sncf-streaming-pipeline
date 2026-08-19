# AKS -- héberge Airflow en production, remplace le conteneur Docker local
# (airflow standalone) par un vrai cluster Kubernetes managé, capable de
# redémarrer automatiquement en cas de problème.
#
# Point de vigilance pour un futur apply réel (jamais rencontré en plan,
# mais à vérifier) : Microsoft.ContainerService doit être enregistré sur
# l'abonnement -- l'enregistrement automatique des fournisseurs de
# ressources est désactivé (voir main.tf), à faire manuellement si besoin
# via `az provider register --namespace Microsoft.ContainerService`.

resource "azurerm_kubernetes_cluster" "sncf" {
  name                = "aks-sncf-${var.environment}"
  location            = azurerm_resource_group.sncf.location
  resource_group_name = azurerm_resource_group.sncf.name
  dns_prefix          = "aks-sncf-${var.environment}"

  sku_tier = "Free"

  default_node_pool {
    name           = "default"
    node_count     = 1
    vm_size        = "Standard_D2s_v3"
    vnet_subnet_id = azurerm_subnet.aks.id
  }

  identity {
    type = "SystemAssigned"
  }

  network_profile {
    network_plugin = "kubenet"
  }

  tags = local.common_tags
}
