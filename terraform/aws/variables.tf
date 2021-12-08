variable "aws_region" {
  type = string
}

variable "aws_profile" {
  type = string
}

variable "create_aws_resources" {
  description = "Specify whether to create new AWS resources for Unity Catalog or not."
  type        = bool
}

variable "unity_metastore_bucket" {
  description = <<EOT
  Name of the Unity Catalog root bucket
  This is the default storage location for managed tables in Unity Catalog
  If create_aws_resources = true, a random suffix will be appended to the bucket name
  EOT
  type        = string
}

variable "unity_metastore_iam" {
  description = <<EOT
  The IAM role arn for Unity Catalog, specified only if create_aws_resources = false
  The format should be arn:aws:iam::account:role/role-name-with-path
  The policy & trust relationship needs to follow the documentation
  EOT
  type        = string
  default = ""
}

variable "databricks_workspace_ids" {
  description = "List of Databricks workspace ids to be enabled with Unity Catalog"
  type        = list[string]
}

variable "databricks_users" {
  description = "List of Databricks users to be added at account-level for Unity Catalog"
  type        = list[string]
}

variable "databricks_account_id" {
  description = "Account Id that could be found in the bottom left corner of https://accounts.cloud.databricks.com/. Not your AWS account id, or Databricks workspace id"
  type        = string
}

variable "databricks_account_username" {
  description = "Databricks account owner credentials"
  type        = string
}

variable "databricks_account_password" {
  description = "Databricks account owner credentials"
  type        = string
}
