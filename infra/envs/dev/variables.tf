variable "capacity_name" {
  description = "Display name of the existing Fabric capacity for DEV."
  type        = string
}

variable "enable_workspace_identity" {
  description = "Create a system-assigned workspace identity."
  type        = bool
  default     = false
}

variable "role_assignments" {
  description = "Workspace role assignments (see module docs)."
  type = map(object({
    principal_id   = string
    principal_type = string
    role           = string
  }))
  default = {}
}
