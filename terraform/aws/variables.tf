variable "aws_region" {
  description = "The AWS Region to deploy into, e.g. us-east-1"
  type = string
}

variable "aws_profile" {
  description = "The AWS Profile to use."
  type = string
}

variable "create_aws_resources" {
  description = "Specify whether to create new AWS resources (S3 bucket, IAM roles) for Unity Catalog or not. Enter true/false"
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
  description = <<EOT
  List of Databricks workspace ids to be enabled with Unity Catalog.
  Enter with square brackets and double quotes
  e.g. ["111111111", "222222222"]
  EOT
  type        = list(string)
}

variable "databricks_users" {
  description = <<EOT
  List of Databricks users to be added at account-level for Unity Catalog.
  Enter with square brackets and double quotes
  e.g ["first.last@domain.com", "second.last@domain.com"]
  EOT
  type        = list(string)
}

variable "databricks_unity_admins" {
  description = <<EOT
  List of Admins to be added at account-level for Unity Catalog.
  Enter with square brackets and double quotes
  e.g ["first.admin@domain.com", "second.admin@domain.com"]
  EOT
  type        = list(string)
}

variable "unity_admin_group" {
  description = "Name of the admin group. This group will be set as the owner of the Unity Catalog metastore"
  type        = string
}

variable "databricks_account_id" {
  description = "Account Id that could be found in the bottom left corner of https://accounts.cloud.databricks.com/. Not your AWS account id, or Databricks workspace id"
  type        = string
}

variable "databricks_workspace_host" {
  description = "Databricks workspace url"
  type        = string
}

variable "databricks_account_username" {
  description = "Databricks account owner credentials"
  type        = string
}

variable "databricks_account_password" {
  description = "Databricks account owner credentials"
  type        = string
  sensitive   = true
}
