terraform {
  required_version = ">= 1.5"

  required_providers {
    snowflake = {
      source  = "snowflakedb/snowflake"
      version = "~> 2.23"
    }
  }
}

# Terraform logs in as the TERRAFORM_SVC service user created by bootstrap.sql.
# It authenticates with a key pair, so there is no password anywhere.
provider "snowflake" {
  organization_name = var.organization_name
  account_name      = var.account_name
  user              = "TERRAFORM_SVC"
  role              = "TERRAFORM"
  authenticator     = "SNOWFLAKE_JWT"
  private_key       = file(pathexpand(var.terraform_private_key_path))
}
