# Printed after every apply. The next steps (Kafka Connect, dbt) need these names.

output "database" {
  value = snowflake_database.wikimedia.name
}

output "raw_schema" {
  value = snowflake_schema.raw.fully_qualified_name
}

output "warehouse" {
  value = snowflake_warehouse.transforming.name
}

output "loader_role" {
  value = snowflake_account_role.loader.name
}

output "transformer_role" {
  value = snowflake_account_role.transformer.name
}

output "kafka_connect_user" {
  value = snowflake_service_user.kafka_connect.name
}
