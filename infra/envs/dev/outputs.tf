output "capacity" {
  value = module.fabric_platform.capacity
}

output "workspace_id" {
  value = module.fabric_platform.workspace_id
}

output "workspace_name" {
  value = module.fabric_platform.workspace_name
}

output "bronze_lakehouse_id" {
  value = module.fabric_platform.bronze_lakehouse_id
}

output "silver_lakehouse_id" {
  value = module.fabric_platform.silver_lakehouse_id
}

output "gold_warehouse_id" {
  value = module.fabric_platform.gold_warehouse_id
}

output "gold_warehouse_connection_string" {
  value = module.fabric_platform.gold_warehouse_connection_string
}
