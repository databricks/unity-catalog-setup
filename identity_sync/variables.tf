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

variable "databricks_groups" {
  description = <<EOT
  List of workspace groups to be added at account-level for Unity Catalog.
  Enter with square brackets and double quotes
  e.g ["Data Engineer", "Data Scientist"]
  EOT
  type        = list(string)
}