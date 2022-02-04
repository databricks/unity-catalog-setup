variable "rg_name" {
  description = "Enter your resource group name"
  type        = string
}

variable "location" {
  description = "Enter your location, i.e. West US or East US"
  type        = string
}

variable "reuse_rg" {
  description = "Reuse resource group, do not create a new resource group (enter true/false)"
  type        = bool
}

variable "tenant_id" {
  description = "Enter your tenant id from Azure Portal"
  type        = string
}

variable "subscription_id" {
  description = "Enter your subscription id from Azure Portal"
  type        = string
}

variable "prefix" {
  description = "Enter a prefix to prepend to any created resources"
  type        = string
}

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

variable "workspace_ids" {
  description = <<EOT
  List of Databricks workspace ids to be enabled with Unity Catalog
  Enter with square brackets and double quotes
  e.g. ["111111111", "222222222"]  
  EOT
  type        = list(string)
}

variable "tags" {
  description = "Enter a dictionary of tags to be added to any resources created"
  default     = {}
  type        = map(any)
}
