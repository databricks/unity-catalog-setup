terraform {
  required_providers {
    databricks = {
      source  = "databrickslabs/databricks"
      version = "~>0.3.12"
    }
    aws = {
      source  = "hashicorp/aws"
      version = "~>3.68.0"
    }
  }
}

provider "aws" {
  region  = var.aws_region
  profile = var.aws_profile

  default_tags {
    tags = {
      Vendor = "Databricks Unity Catalog"
    }
  }
}

// initialize provider in "MWS" mode, to add users at account-level
provider "databricks" {
  alias      = "mws"
  host       = "https://accounts.cloud.databricks.com"
  account_id = var.databricks_account_id
  username   = var.databricks_account_username
  password   = var.databricks_account_password
}

// initialize provider at workspace level, to create UC resources
// authentication using one of the described approaches at
// https://registry.terraform.io/providers/databrickslabs/databricks/latest/docs#authentication
provider "databricks" {
  alias   = "workspace"
  profile = "UC-DEC"
}
