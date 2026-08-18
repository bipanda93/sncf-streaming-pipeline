output "resource_group_name" {
  description = "Nom du groupe de ressources créé"
  value       = azurerm_resource_group.sncf.name
}

output "resource_group_location" {
  value = azurerm_resource_group.sncf.location
}
