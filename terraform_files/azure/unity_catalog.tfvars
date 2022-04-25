tenant_id       = "00000000-0000-0000-0000-000000000000"
subscription_id = "00000000-0000-0000-0000-00000000000"

reuse_rg = true
rg_name  = "unity-catalog-testing"
location = "East US"

prefix = "uc"

tags = {
  Owner       = "you@yourcompany.comm"
  environment = "uc-testing"
}

metastore_name  = "zpuc"
metastore_owner = "yourgroup"
catalog_name    = "awesomecatalog"
schema_name     = "awesomeschema"
workspace_ids   = ["0000000000"]

databricks_workspace_host = "https://adb-00000000000.0.azuredatabricks.net/"