
variable "unity_metastore_bucket" {
  description = "UC metastore bucket"
  type        = string
}

variable "unity_metastore_iam" {
  description = "UC metastore IAM role"
  type        = string
}

variable "databricks_workspace_ids" {
  description = "List of Databricks workspace ids to be enabled with Unity Catalog"
  type        = list(string)
}

variable "unity_admin_group" {
  description = "Group to be set as owner of the metastore"
  type        = string
}
