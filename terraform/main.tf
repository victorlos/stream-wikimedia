resource "snowflake_database" "wikimedia" {
  name    = "WIKIMEDIA"
  comment = "Wikimedia recent changes, streamed through Kafka. Managed by Terraform."
}

resource "snowflake_schema" "raw" {
  database = snowflake_database.wikimedia.name
  name     = "RAW"
  comment  = "Events exactly as they arrive from Kafka."
}

resource "snowflake_warehouse" "transforming" {
  name                = "TRANSFORMING_WH"
  warehouse_size      = "XSMALL"
  auto_suspend        = 60 # seconds of inactivity before it stops billing
  auto_resume         = "true"
  initially_suspended = true
  comment             = "dbt and ad hoc queries. Managed by Terraform."
}
