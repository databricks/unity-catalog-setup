data "databricks_spark_version" "latest" {
}
data "databricks_node_type" "smallest" {
  local_disk = true
}

resource "databricks_cluster" "unity_sql" {
  cluster_name            = "Unity SQL"
  spark_version           = data.databricks_spark_version.latest.id
  node_type_id            = data.databricks_node_type.smallest.id
  autotermination_minutes = 60
  enable_elastic_disk     = false
  num_workers             = 2
  azure_attributes {
    availability = "SPOT"
  }
  data_security_mode = "USER_ISOLATION"
}
