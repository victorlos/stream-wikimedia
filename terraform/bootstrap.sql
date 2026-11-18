-- One-time setup. Run in a Snowsight worksheet as ACCOUNTADMIN, before Terraform.
-- Everything after this is created by Terraform.

USE ROLE ACCOUNTADMIN;

-- 1. A role and a service user for Terraform -------------------------------
--
-- TERRAFORM inherits SYSADMIN (create databases and warehouses) and
-- SECURITYADMIN (create roles and users, manage grants). It does not get
-- ACCOUNTADMIN.

CREATE ROLE IF NOT EXISTS TERRAFORM
  COMMENT = 'Used by Terraform to manage the wiki-streaming project.';

GRANT ROLE SYSADMIN      TO ROLE TERRAFORM;
GRANT ROLE SECURITYADMIN TO ROLE TERRAFORM;

-- Paste the public key as a single line, without the BEGIN/END lines.
CREATE USER IF NOT EXISTS TERRAFORM_SVC
  TYPE           = SERVICE
  DEFAULT_ROLE   = TERRAFORM
  RSA_PUBLIC_KEY = '<PASTE terraform_svc.pub HERE>'
  COMMENT        = 'Terraform service user. Key-pair authentication only.';

GRANT ROLE TERRAFORM TO USER TERRAFORM_SVC;

-- 2. A spending limit --------------------------------------------------------
--
-- Suspends every warehouse in the account once 20 credits are used in a month.
-- This applies to the WHOLE ACCOUNT. Skip this block, or raise the quota, if
-- the account is also used for other work.
-- It does not cap serverless features such as Snowpipe Streaming.

CREATE RESOURCE MONITOR IF NOT EXISTS ACCOUNT_MONTHLY_LIMIT
  WITH CREDIT_QUOTA    = 20
       FREQUENCY       = MONTHLY
       START_TIMESTAMP = IMMEDIATELY
  TRIGGERS ON 75  PERCENT DO NOTIFY
           ON 100 PERCENT DO SUSPEND;

ALTER ACCOUNT SET RESOURCE_MONITOR = ACCOUNT_MONTHLY_LIMIT;
