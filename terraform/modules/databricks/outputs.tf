output "catalog_name" {
  value = databricks_catalog.pipeline.name
}

output "bronze_schema" {
  value = databricks_schema.bronze.name
}

output "silver_schema" {
  value = databricks_schema.silver.name
}

output "gold_schema" {
  value = databricks_schema.gold.name
}

output "sql_warehouse_id" {
  value = databricks_sql_endpoint.gold_warehouse.id
}
