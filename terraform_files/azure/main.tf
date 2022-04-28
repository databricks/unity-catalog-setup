/*****************************************************************************************************************
* Create a Unity Catalog metastore with its default root bucket & service principal
* Create securables inside the newly created metastore: catalog, schema, external location & credentials
* Create a UC-enabled cluster
******************************************************************************************************************/

resource "random_string" "naming" {
  special = false
  upper   = false
  length  = 6
}

locals {
  prefix = format("%s%s", var.prefix, random_string.naming.result)
}

resource "azurerm_resource_group" "unity_catalog" {
  count    = var.reuse_rg ? 0 : 1
  name     = var.rg_name
  location = var.location
}

data "azurerm_resource_group" "unity_catalog" {
  count = var.reuse_rg ? 1 : 0
  name  = var.rg_name
}

resource "azurerm_storage_account" "unity_catalog" {
  name                     = local.prefix
  resource_group_name      = var.reuse_rg ? data.azurerm_resource_group.unity_catalog[0].name : azurerm_resource_group.unity_catalog[0].name
  location                 = var.reuse_rg ? data.azurerm_resource_group.unity_catalog[0].location : azurerm_resource_group.unity_catalog[0].location
  account_tier             = "Standard"
  account_replication_type = "GRS"
  is_hns_enabled           = true

  tags = var.tags
}

resource "azurerm_storage_container" "unity_catalog" {
  name                  = local.prefix
  storage_account_name  = azurerm_storage_account.unity_catalog.name
  container_access_type = "private"
}

resource "azurerm_role_assignment" "example" {
  scope                = azurerm_storage_account.unity_catalog.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azuread_service_principal.unity_catalog.object_id
}
