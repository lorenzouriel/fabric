output "capacity" {
  description = "Capacity the workspace is assigned to."
  value = {
    id     = data.fabric_capacity.this.id
    sku    = data.fabric_capacity.this.sku
    region = data.fabric_capacity.this.region
  }
}

output "workspace_id" {
  description = "Fabric workspace ID."
  value       = fabric_workspace.this.id
}

output "workspace_name" {
  description = "Fabric workspace display name."
  value       = fabric_workspace.this.display_name
}

output "bronze_lakehouse_id" {
  description = "lh_senado_bronze item ID."
  value       = fabric_lakehouse.this["bronze"].id
}

output "silver_lakehouse_id" {
  description = "lh_senado_silver item ID."
  value       = fabric_lakehouse.this["silver"].id
}

output "gold_warehouse_id" {
  description = "wh_senado_gold item ID."
  value       = fabric_warehouse.gold.id
}

output "gold_warehouse_connection_string" {
  description = "SQL endpoint for wh_senado_gold."
  value       = fabric_warehouse.gold.properties.connection_string
}
