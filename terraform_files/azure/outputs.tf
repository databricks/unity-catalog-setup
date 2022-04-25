output "sp_application_id" {
  value = azuread_application.unity_catalog.application_id
}

output "sp_directory_id" {
  value = var.tenant_id
}

output "sp_secret" {
  value     = azuread_application_password.unity_catalog.value
  sensitive = true
}

output "storage_path" {
  value = format("abfss://%s@%s.dfs.core.windows.net/",
    azurerm_storage_account.unity_catalog.name,
    azurerm_storage_container.unity_catalog.name
  )
}
