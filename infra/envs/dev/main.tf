module "fabric_platform" {
  source = "../../modules/fabric_platform"

  environment               = "dev"
  capacity_name             = var.capacity_name
  enable_workspace_identity = var.enable_workspace_identity
  role_assignments          = var.role_assignments
}
