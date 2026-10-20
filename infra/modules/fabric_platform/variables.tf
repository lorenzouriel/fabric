variable "environment" {
  description = "Environment short name (dev, test, prod). Used in the workspace name."
  type        = string

  validation {
    condition     = contains(["dev", "test", "prod"], var.environment)
    error_message = "environment must be one of: dev, test, prod."
  }
}

variable "project_name" {
  description = "Project short name used as the workspace name prefix."
  type        = string
  default     = "senado360"
}

variable "capacity_name" {
  description = "Display name of an existing Fabric capacity (trial or F-SKU). Terraform only reads it."
  type        = string
}

variable "enable_workspace_identity" {
  description = "Create a system-assigned workspace identity (needed later for Key Vault access in E13.S2)."
  type        = bool
  default     = false
}

variable "enable_lakehouse_schemas" {
  description = "Create the Bronze and Silver lakehouses with schemas enabled. Cannot be changed after creation."
  type        = bool
  default     = true
}

variable "warehouse_collation" {
  description = "Gold warehouse collation. Changing it forces the warehouse to be recreated."
  type        = string
  default     = "Latin1_General_100_CI_AS_KS_WS_SC_UTF8"

  validation {
    condition = contains([
      "Latin1_General_100_BIN2_UTF8",
      "Latin1_General_100_CI_AS_KS_WS_SC_UTF8",
    ], var.warehouse_collation)
    error_message = "warehouse_collation must be Latin1_General_100_BIN2_UTF8 or Latin1_General_100_CI_AS_KS_WS_SC_UTF8."
  }
}

variable "role_assignments" {
  description = <<-EOT
    Workspace role assignments keyed by a stable label. Prefer Entra groups over users.
    The identity running `terraform apply` becomes workspace Admin automatically; do not list it here.
  EOT
  type = map(object({
    principal_id   = string
    principal_type = string
    role           = string
  }))
  default = {}

  validation {
    condition = alltrue([
      for ra in values(var.role_assignments) :
      contains(["Group", "ServicePrincipal", "ServicePrincipalProfile", "User"], ra.principal_type)
      && contains(["Admin", "Member", "Contributor", "Viewer"], ra.role)
    ])
    error_message = "principal_type must be Group|ServicePrincipal|ServicePrincipalProfile|User and role must be Admin|Member|Contributor|Viewer."
  }
}
