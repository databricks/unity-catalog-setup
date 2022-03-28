terraform {
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~>2.99.0"
    }
    azuread = {
      source  = "hashicorp/azuread"
      version = "~>2.19.0"
    }
    databricks = {
      source  = "databrickslabs/databricks"
      version = "~>0.5.2"
    }
  }
}

provider "azuread" {
  tenant_id = local.tenant_id
}

provider "azurerm" {
  subscription_id = local.subscription_id
  features {}
}

provider "databricks" {
  host = local.databricks_workspace_host
}
