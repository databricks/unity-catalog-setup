
resource "databricks_metastore" "unity" {
  name          = "unity-catalog-tf"
  storage_root  = "s3://${var.unity_metastore_bucket}"
  force_destroy = true
}

resource "databricks_metastore_data_access" "default_dac" {
  metastore_id = databricks_metastore.unity.id
  name         = "default_dac"
  is_default   = true
  aws_iam_role {
    role_arn = var.unity_metastore_iam
  }
}

resource "databricks_metastore_assignment" "default_metastore" {
  for_each     = toset(var.databricks_workspace_ids)
  workspace_id = each.key
  metastore_id = databricks_metastore.unity.id
}

