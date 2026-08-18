variable "environment" {
  description = "Environnement de déploiement (dev, prod)"
  type        = string
  default     = "dev"
}

variable "location" {
  description = "Région Azure"
  type        = string
  default     = "francecentral"
}

variable "project_name" {
  description = "Nom court du projet, utilisé dans le nommage des ressources"
  type        = string
  default     = "sncfstream"
}
