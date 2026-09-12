# Azure Container Registry -- stocke l'image Docker Airflow
# (Dockerfile.airflow) pour qu'AKS puisse la tirer depuis le cluster. Sans
# registre, AKS n'a aucun moyen d'accéder à une image qui n'existe que sur
# le disque local.

resource "azurerm_container_registry" "sncf" {
  # Le nom compose l'URL publique du registre (acrsncfdev.azurecr.io) --
  # doit être unique sur toute la plateforme Azure, pas juste cet
  # abonnement, même principe que le nom du compte de stockage
  # (storage.tf).
  name                = "acrsncf${var.environment}"
  resource_group_name = azurerm_resource_group.sncf.name
  location            = azurerm_resource_group.sncf.location

  # Basic suffit largement pour ce projet (une poignée d'images, pas de
  # trafic de tirage simultané élevé) -- contrairement à Event Hubs,
  # aucune fonctionnalité n'est bloquée sur ce palier, c'est un choix
  # économique pur, pas structurel.
  sku = "Basic"

  # Désactive le compte administrateur intégré (identifiant/mot de passe
  # statiques) -- cohérent avec le reste du projet (Key Vault, identités
  # managées) : jamais de identifiants statiques quand une identité
  # managée peut faire le travail. AKS s'authentifiera via son identité
  # kubelet (voir iam.tf, aks_to_acr) plutôt qu'un compte admin.
  admin_enabled = false

  tags = local.common_tags
}
