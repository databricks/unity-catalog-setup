data "databricks_user" "users" {
  provider = databricks.workspace
  for_each = data.databricks_group.users.members
  user_id  = each.key
}

data "databricks_user" "admins" {
  provider = databricks.workspace
  for_each = data.databricks_group.admins.members
  user_id  = each.key
}

// create account-level users
resource "databricks_user" "unity_users" {
  provider  = databricks.account
  for_each  = data.databricks_user.users
  user_name = each.value.user_name
  force     = true
}