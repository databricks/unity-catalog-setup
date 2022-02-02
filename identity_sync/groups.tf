data "databricks_group" "users" {
  provider     = databricks.workspace
  display_name = "users"
}

data "databricks_group" "admins" {
  provider     = databricks.workspace
  display_name = "admins"
}

data "databricks_group" "ws_groups" {
  provider     = databricks.workspace
  for_each     = toset(var.databricks_groups)
  display_name = each.key
}

// create other specified groups
resource "databricks_group" "acc_groups" {
  provider     = databricks.account
  for_each     = toset(var.databricks_groups)
  display_name = each.value
}

//create the admin group
resource "databricks_group" "admin_group" {
  provider     = databricks.account
  display_name = "unity admins"
}