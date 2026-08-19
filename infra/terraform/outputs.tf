output "resource_group_name" {
  description = "Nom du groupe de ressources créé"
  value       = azurerm_resource_group.sncf.name
}

output "resource_group_location" {
  value = azurerm_resource_group.sncf.location
}

output "vnet_name" {
  value = azurerm_virtual_network.sncf.name
}

output "subnet_databricks_public_id" {
  value = azurerm_subnet.databricks_public.id
}

output "subnet_databricks_private_id" {
  value = azurerm_subnet.databricks_private.id
}

output "subnet_aks_id" {
  value = azurerm_subnet.aks.id
}

output "subnet_private_endpoints_id" {
  value = azurerm_subnet.private_endpoints.id
}

output "key_vault_name" {
  value = azurerm_key_vault.sncf.name
}
