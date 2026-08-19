# Réseau -- fondation pour Databricks (VNet injection) et AKS.
# Voir notes/decisions_architecture.md, section "Réseau", pour la
# justification du choix d'un VNet explicite plutôt qu'un réseau géré
# automatiquement par chaque service.

resource "azurerm_virtual_network" "sncf" {
  name                = "vnet-sncf-streaming-${var.environment}"
  resource_group_name = azurerm_resource_group.sncf.name
  location            = azurerm_resource_group.sncf.location
  address_space       = ["10.0.0.0/16"]

  tags = local.common_tags
}

# Databricks nécessite EXACTEMENT deux sous-réseaux dédiés (VNet injection) :
# un "public" (hôte du driver Spark) et un "privé" (workers), chacun délégué
# à Microsoft.Databricks/workspaces. Les règles NSG spécifiques qu'Azure
# exige pour Databricks seront ajoutées lors de la construction du module
# Databricks lui-même -- ici, juste la structure réseau de base.
resource "azurerm_subnet" "databricks_public" {
  name                 = "snet-databricks-public"
  resource_group_name  = azurerm_resource_group.sncf.name
  virtual_network_name = azurerm_virtual_network.sncf.name
  address_prefixes     = ["10.0.1.0/24"]

  delegation {
    name = "databricks"
    service_delegation {
      name = "Microsoft.Databricks/workspaces"
      actions = [
        "Microsoft.Network/virtualNetworks/subnets/join/action",
        "Microsoft.Network/virtualNetworks/subnets/prepareNetworkPolicies/action",
        "Microsoft.Network/virtualNetworks/subnets/unprepareNetworkPolicies/action",
      ]
    }
  }
}

resource "azurerm_subnet" "databricks_private" {
  name                 = "snet-databricks-private"
  resource_group_name  = azurerm_resource_group.sncf.name
  virtual_network_name = azurerm_virtual_network.sncf.name
  address_prefixes     = ["10.0.2.0/24"]

  delegation {
    name = "databricks"
    service_delegation {
      name = "Microsoft.Databricks/workspaces"
      actions = [
        "Microsoft.Network/virtualNetworks/subnets/join/action",
        "Microsoft.Network/virtualNetworks/subnets/prepareNetworkPolicies/action",
        "Microsoft.Network/virtualNetworks/subnets/unprepareNetworkPolicies/action",
      ]
    }
  }
}

# Sous-réseau dédié à AKS (héberge Airflow).
resource "azurerm_subnet" "aks" {
  name                 = "snet-aks"
  resource_group_name  = azurerm_resource_group.sncf.name
  virtual_network_name = azurerm_virtual_network.sncf.name
  address_prefixes     = ["10.0.3.0/24"]
}

# Sous-réseau réservé aux points de terminaison privés (ADLS Gen2, Event
# Hubs, Key Vault) -- permet à Databricks/AKS de les joindre sans jamais
# transiter par internet public.
resource "azurerm_subnet" "private_endpoints" {
  name                 = "snet-private-endpoints"
  resource_group_name  = azurerm_resource_group.sncf.name
  virtual_network_name = azurerm_virtual_network.sncf.name
  address_prefixes     = ["10.0.4.0/24"]
}

# Databricks exige qu'un NSG soit associé aux deux sous-réseaux dédiés,
# même avant l'ajout des règles spécifiques (faites lors du module
# Databricks). Association vide pour l'instant -- structure uniquement.
resource "azurerm_network_security_group" "databricks_public" {
  name                = "nsg-databricks-public-${var.environment}"
  resource_group_name = azurerm_resource_group.sncf.name
  location            = azurerm_resource_group.sncf.location
  tags                = local.common_tags
}

resource "azurerm_network_security_group" "databricks_private" {
  name                = "nsg-databricks-private-${var.environment}"
  resource_group_name = azurerm_resource_group.sncf.name
  location            = azurerm_resource_group.sncf.location
  tags                = local.common_tags
}

resource "azurerm_subnet_network_security_group_association" "databricks_public" {
  subnet_id                 = azurerm_subnet.databricks_public.id
  network_security_group_id = azurerm_network_security_group.databricks_public.id
}

resource "azurerm_subnet_network_security_group_association" "databricks_private" {
  subnet_id                 = azurerm_subnet.databricks_private.id
  network_security_group_id = azurerm_network_security_group.databricks_private.id
}
