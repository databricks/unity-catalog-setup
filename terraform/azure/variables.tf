variable "metastore_name" {
  description = "Enter the name of the metastore, it can be scoped by environment or LOB i.e. dev/prod/sales/engr"
  type        = string
}

variable "metastore_owner" {
  description = "Enter the name of the metastore owner, this must be a valid email account or group name in Azure Active Directory"
  type        = string
}

variable "catalog_name" {
  description = "Enter the name of a default catalog to create"
  type        = string
}

variable "schema_name" {
  description = "Enter the name of a default schema to create"
  type        = string
}

variable "databricks_resource_id" {
  description = "The Azure resource ID for the databricks workspace deployment."
}

