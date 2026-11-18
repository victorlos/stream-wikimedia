# Who can do what. Two roles, one per job:
#   WIKI_LOADER       writes raw events (the Kafka connector)
#   WIKI_TRANSFORMER  reads raw events and builds models (dbt, and you)

locals {
  # Snowflake wants the public key as one line, without the BEGIN/END lines.
  kafka_public_key = join("", [
    for line in split("\n", file(pathexpand(var.kafka_public_key_path))) :
    trimspace(line) if !startswith(line, "-----")
  ])
}

# ---------------------------------------------------------------- loader

resource "snowflake_account_role" "loader" {
  name    = "WIKI_LOADER"
  comment = "Writes raw events into WIKIMEDIA.RAW."
}

resource "snowflake_grant_privileges_to_account_role" "loader_database" {
  account_role_name = snowflake_account_role.loader.name
  privileges        = ["USAGE"]
  on_account_object {
    object_type = "DATABASE"
    object_name = snowflake_database.wikimedia.name
  }
}

# The connector creates its own table, plus a stage and a pipe when it needs them.
resource "snowflake_grant_privileges_to_account_role" "loader_raw_schema" {
  account_role_name = snowflake_account_role.loader.name
  privileges        = ["USAGE", "CREATE TABLE", "CREATE STAGE", "CREATE PIPE"]
  on_schema {
    schema_name = snowflake_schema.raw.fully_qualified_name
  }
}

resource "snowflake_service_user" "kafka_connect" {
  name           = "KAFKA_CONNECT_SVC"
  default_role   = snowflake_account_role.loader.name
  rsa_public_key = local.kafka_public_key
  comment        = "Kafka Connect sink. Key-pair authentication only."
}

resource "snowflake_grant_account_role" "loader_to_kafka_connect" {
  role_name = snowflake_account_role.loader.name
  user_name = snowflake_service_user.kafka_connect.name
}

# ----------------------------------------------------------- transformer

resource "snowflake_account_role" "transformer" {
  name    = "WIKI_TRANSFORMER"
  comment = "Reads WIKIMEDIA.RAW and builds dbt models."
}

resource "snowflake_grant_privileges_to_account_role" "transformer_warehouse" {
  account_role_name = snowflake_account_role.transformer.name
  privileges        = ["USAGE"]
  on_account_object {
    object_type = "WAREHOUSE"
    object_name = snowflake_warehouse.transforming.name
  }
}

# CREATE SCHEMA lets dbt create the schemas it builds models in.
resource "snowflake_grant_privileges_to_account_role" "transformer_database" {
  account_role_name = snowflake_account_role.transformer.name
  privileges        = ["USAGE", "CREATE SCHEMA"]
  on_account_object {
    object_type = "DATABASE"
    object_name = snowflake_database.wikimedia.name
  }
}

resource "snowflake_grant_privileges_to_account_role" "transformer_raw_schema" {
  account_role_name = snowflake_account_role.transformer.name
  privileges        = ["USAGE"]
  on_schema {
    schema_name = snowflake_schema.raw.fully_qualified_name
  }
}

# The raw table does not exist yet, so grant SELECT on whatever tables appear.
resource "snowflake_grant_privileges_to_account_role" "transformer_raw_tables" {
  account_role_name = snowflake_account_role.transformer.name
  privileges        = ["SELECT"]
  on_schema_object {
    future {
      object_type_plural = "TABLES"
      in_schema          = snowflake_schema.raw.fully_qualified_name
    }
  }
}

resource "snowflake_grant_account_role" "transformer_to_developer" {
  role_name = snowflake_account_role.transformer.name
  user_name = var.developer_user
}

# ------------------------------------------------------------- hierarchy

# Snowflake's convention: custom roles roll up to SYSADMIN, so administrators
# can see and manage everything the project creates.
resource "snowflake_grant_account_role" "loader_to_sysadmin" {
  role_name        = snowflake_account_role.loader.name
  parent_role_name = "SYSADMIN"
}

resource "snowflake_grant_account_role" "transformer_to_sysadmin" {
  role_name        = snowflake_account_role.transformer.name
  parent_role_name = "SYSADMIN"
}
