resource "databricks_metastore" "this" {
  name = var.metastore_name // needs to come from a var, default to uc
  storage_root = format("abfss://%s@%s.dfs.core.windows.net/",
    azurerm_storage_account.unity_catalog.name,
    azurerm_storage_container.unity_catalog.name)
  owner = var.metastore_owner // needs to be an ad group that exists, comes from a var
  // forcefully remove that auto-created
  // catalog we have no access to
  force_destroy = true
}

resource "databricks_metastore_data_access" "first" {
  metastore_id = databricks_metastore.this.id
  name = "the-keys"
  azure_service_principal {
    directory_id = var.tenant_id
    application_id = azuread_application.unity_catalog.application_id
    client_secret = azuread_application_password.unity_catalog.value
  }

  // added this argument here, as we have
  // a cyclic dependency between entities and
  // it's the best way around it
  is_default = true
}

resource "databricks_metastore_assignment" "this" {
  metastore_id = databricks_metastore.this.id
  // TODO: add a variable for a workspace
  workspace_id = var.workspace_id //
}

resource "databricks_catalog" "catalog" {
  metastore_id = databricks_metastore.this.id
  name = var.catalog_name // needs to come from a var, default to sandbox
  comment = "this catalog is managed by terraform"
  properties = {
    purpose = "testing"
  }
}

resource "databricks_schema" "things" {
  catalog_name = databricks_catalog.catalog.id
  name = var.schema_name // needs to come from a var, default to things
 
  comment = "this database is managed by terraform"
  properties = {
    kind = "various"
  }
}