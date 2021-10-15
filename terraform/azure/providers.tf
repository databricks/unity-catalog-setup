terraform {
  required_providers {
    azurerm = {
      source = "hashicorp/azurerm"
      version = "2.80.0"
    }      
    azuread = {
      source = "hashicorp/azuread"
      version = "2.6.0"
    }
    databricks = {
      source  = "databrickslabs/databricks"
      version = "0.3.8"
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