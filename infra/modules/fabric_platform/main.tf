locals {
  workspace_name = "${var.project_name}-${var.environment}"

  # Item names are identical across environments so deployment pipelines can pair them.
  lakehouses = {
    bronze = {
      display_name = "lh_senado_bronze"
      description  = "Bronze layer: raw, immutable source data and audit trail."
    }
    silver = {
      display_name = "lh_senado_silver"
      description  = "Silver layer: clean, conformed Delta entities."
    }
  }
}

data "fabric_capacity" "this" {
  display_name = var.capacity_name

  lifecycle {
    postcondition {
      condition     = self.state == "Active"
      error_message = "Capacity '${var.capacity_name}' is not Active (paused or expired). Resume it before applying."
    }

    # Lakehouses and warehouses need a Fabric capacity (F-SKU, or trial FT/FTL-SKU such as FTL64);
    # Power BI PPU/P/A SKUs cannot host them.
    postcondition {
      condition     = can(regex("^F(TL?)?[0-9]+$", self.sku))
      error_message = "Capacity '${var.capacity_name}' has SKU '${self.sku}', which is not a Fabric capacity. Use a Fabric trial (e.g. FTL64) or an F-SKU."
    }
  }
}

resource "fabric_workspace" "this" {
  display_name = local.workspace_name
  description  = "Senado 360: Brazilian Senate Data & AI Platform (${var.environment})."
  capacity_id  = data.fabric_capacity.this.id

  identity = var.enable_workspace_identity ? { type = "SystemAssigned" } : null
}

resource "fabric_lakehouse" "this" {
  for_each = local.lakehouses

  workspace_id = fabric_workspace.this.id
  display_name = each.value.display_name
  description  = each.value.description

  configuration = {
    enable_schemas = var.enable_lakehouse_schemas
  }
}

resource "fabric_warehouse" "gold" {
  workspace_id = fabric_workspace.this.id
  display_name = "wh_senado_gold"
  description  = "Gold layer: dimensional model, analytical views and AI serving views."

  configuration = {
    collation_type = var.warehouse_collation
  }
}

resource "fabric_workspace_role_assignment" "this" {
  for_each = var.role_assignments

  workspace_id = fabric_workspace.this.id
  role         = each.value.role

  principal = {
    id   = each.value.principal_id
    type = each.value.principal_type
  }
}
