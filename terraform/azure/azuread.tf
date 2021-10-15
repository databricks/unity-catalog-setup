resource "azuread_application" "unity_catalog" {
  display_name = local.prefix
}

resource "azuread_application_password" "unity_catalog" {
  application_object_id = azuread_application.unity_catalog.object_id
}

resource "azuread_service_principal" "unity_catalog" {
  application_id               = azuread_application.unity_catalog.application_id
  app_role_assignment_required = false
}
