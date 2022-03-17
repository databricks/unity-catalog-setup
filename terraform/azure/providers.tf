terraform {
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~>2.92.0"
    }
    azuread = {
      source  = "hashicorp/azuread"
      version = "~>2.15.0"
    }
    databricks = {
      source  = "databrickslabs/databricks"
      version = "~>0.4.0"
    }
  }
}

provider "azuread" {
  tenant_id = var.tenant_id
}

provider "azurerm" {
  subscription_id = var.subscription_id
  features {}
}

provider "databricks" {
  host = var.databricks_workspace_host
}
