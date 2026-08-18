locals {
  common_tags = {
    project     = "sncf-streaming-pipeline"
    environment = var.environment
    managed_by  = "terraform"
    owner       = "franck-bipanda"
  }
}
