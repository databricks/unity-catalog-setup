resource "azuread_application" "ext_cred" {
  display_name = "${local.prefix}-cred"
}

resource "azuread_application_password" "ext_cred" {
  application_object_id = azuread_application.ext_cred.object_id
}

resource "azuread_service_principal" "ext_cred" {
  application_id               = azuread_application.ext_cred.application_id
  app_role_assignment_required = false
}

resource "azurerm_storage_account" "ext_storage" {
  name                     = "${local.prefix}ext"
  resource_group_name      = var.reuse_rg ? data.azurerm_resource_group.unity_catalog[0].name : azurerm_resource_group.unity_catalog[0].name
  location                 = var.reuse_rg ? data.azurerm_resource_group.unity_catalog[0].location : azurerm_resource_group.unity_catalog[0].location
  account_tier             = "Standard"
  account_replication_type = "GRS"
  is_hns_enabled           = true

  tags = var.tags
}

resource "azurerm_storage_container" "ext_storage" {
  name                  = "${local.prefix}-ext"
  storage_account_name  = azurerm_storage_account.ext_storage.name
  container_access_type = "private"
}

resource "azurerm_role_assignment" "ext_storage" {
  scope                = azurerm_storage_account.ext_storage.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azuread_service_principal.ext_cred.object_id
}

resource "databricks_storage_credential" "external" {
  name = azuread_application.ext_cred.display_name
  azure_service_principal {
    directory_id   = var.tenant_id
    application_id = azuread_application.ext_cred.application_id
    client_secret  = azuread_application_password.ext_cred.value
  }
  comment = "Managed by TF"
  depends_on = [
    databricks_metastore_assignment.this
  ]
}

resource "databricks_external_location" "some" {
  name = "external"
  url = format("abfss://%s@%s.dfs.core.windows.net",
    azurerm_storage_account.ext_storage.name,
  azurerm_storage_container.ext_storage.name)
  credential_name = databricks_storage_credential.external.id
  comment         = "Managed by TF"
  depends_on = [
    databricks_metastore_assignment.this
  ]
}
