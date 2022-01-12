variable "rg_name" {
  description = "Enter your resource group name"
  type = string
}

variable "location" {
  description = "Enter your location, i.e. West US or East US"
  type = string
}

variable "reuse_rg" {
  description = "Reuse resource group, do not create a new resource group"
  type = bool
}

variable "tenant_id" {
  description = "Enter your tenant id from Azure Portal"
  type = string
}

variable "subscription_id" {
  description = "Enter your subscription id from Azure Portal"
  type = string
}

variable "prefix" {
  description = "Enter a prefix to prepend to any created resources"
  type = string
}

variable tags {
  description = "Enter a dictionary of tags to be added to any resources created"
  default = {}
  type = map
}