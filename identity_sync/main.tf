//add workspace admins to a new group, to act as metastore owner
resource "databricks_group_member" "admin_group_member" {
  provider  = databricks.account
  for_each  = data.databricks_user.admins
  group_id  = databricks_group.admin_group.id
  member_id = each.value.id
}

//sync admins from workspace to account-level
resource "databricks_user_role" "my_user_account_admin" {
  provider = databricks.account
  for_each = data.databricks_user.admins
  user_id  = each.value.id
  role     = "account_admin"
}

// put users to respective groups
resource "databricks_group_member" "this" {
  provider = databricks.account
  for_each = toset(flatten(
    [for group_name in toset(var.databricks_groups) :
      [for member_id in data.databricks_group.ws_groups[group_name].members :
        jsonencode({
          user : member_id,
          group : group_name
  })]]))
  group_id  = databricks_group.acc_groups[jsondecode(each.value).group].id
  member_id = databricks_user.unity_users[jsondecode(each.value).user].id
}
