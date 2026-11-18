# Your account identifier is "<organization>-<account>". In Snowsight, open the
# account menu (bottom left) and choose "View account details" to find both parts.

variable "organization_name" {
  description = "First part of the Snowflake account identifier."
  type        = string
}

variable "account_name" {
  description = "Second part of the Snowflake account identifier."
  type        = string
}

variable "developer_user" {
  description = "Your own Snowflake user name. It receives the transformer role so you can run dbt."
  type        = string
}

variable "terraform_private_key_path" {
  description = "Private key of the TERRAFORM_SVC user."
  type        = string
  default     = "~/.snowflake/wiki-streaming/terraform_svc.p8"
}

variable "kafka_public_key_path" {
  description = "Public key of the Kafka Connect service user."
  type        = string
  default     = "~/.snowflake/wiki-streaming/kafka_connect_svc.pub"
}
