# Databricks notebook source
dbutils.widgets.removeAll()

# COMMAND ----------

# DBTITLE 1,After running this cell, select your Cloud and Runtime Version
dbutils.widgets.dropdown("cloud", "Select one", ["Select one", "AWS", "Azure"])
dbutils.widgets.dropdown("runtime", "Select one", ["Select one","Standard - SQL Only", "Standard - Multi Language", "Machine Learning - Multi Language", "SQL Endpoint"])

# COMMAND ----------

# DBTITLE 1,Run all the remaining cells!
import uuid
import requests
import json
import time

cloud = dbutils.widgets.get("cloud")
runtime = dbutils.widgets.get("runtime")

# COMMAND ----------

#### DOUBLE-CHECK THE CLUSTER IMAGES #####
spark_version = "10.1.x-scala2.12"
sql_photon_version = "10.1.x-photon-scala2.12"
mlr_version = "10.1.x-cpu-ml-scala2.12"

if runtime=="Machine Learning - Multi Language":
    image = mlr_version
    cluster_name = "uc-mlr-multi-language-cluster-" + uuid.uuid4().hex[:8]
elif runtime=="Standard - SQL Only":
    image = spark_version
    cluster_name = "uc-sql-cluster-" + uuid.uuid4().hex[:8]
elif runtime=="Standard - Multi Language":
    image = spark_version
    cluster_name = "uc-multi-language-cluster-" + uuid.uuid4().hex[:8]    
else:
    image = sql_photon_version
    cluster_name = "uc-endpoint-" + uuid.uuid4().hex[:8]

# COMMAND ----------

host = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().getOrElse(None)
token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().getOrElse(None)
user = dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().get("user").getOrElse(None)
workspace_id = dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().get("orgId").getOrElse(None)

# COMMAND ----------

# MAGIC  %md
# MAGIC  ## Create a UC-enabled cluster

# COMMAND ----------

# declare cluster config (need to account for AWS & Azure VMs)

cluster_json = {
    "num_workers": 2,
    "cluster_name": cluster_name,
    "spark_version": image,
    "spark_conf": {
        "spark.databricks.unityCatalog.enabled": "true",
        "spark.databricks.unityCatalog.enforce.permissions": "false",
        "spark.databricks.passthrough.enabled": "false"
    },
    "ssh_public_keys": [],
    "custom_tags": {},
    "spark_env_vars": {
        "PYSPARK_PYTHON": "/databricks/python3/bin/python3"
    },
    "autotermination_minutes": 120,
    "cluster_source": "UI",
    "init_scripts": []
}

if cloud == "AWS":
    cluster_json["node_type_id"] = "i3.xlarge"
    cluster_json["aws_attributes"] = {
        "first_on_demand": 1,
        "availability": "SPOT_WITH_FALLBACK",
        "spot_bid_price_percent": 100,
        "ebs_volume_count": 0
    }
    
elif cloud == "Azure":
    cluster_json["azure_attributes"] = {
        "first_on_demand": 1,
        "availability": "ON_DEMAND_AZURE",
        "spot_bid_max_price": -1
    }
    cluster_json["enable_elastic_disk"] = True
    cluster_json["node_type_id"] = "Standard_DS3_v2"
    cluster_json["driver_node_type_id"] = "Standard_DS3_v2"
    
if runtime in ["Standard - Multi Language", "Machine Learning - Multi Language"]:
    cluster_json["single_user_name"] = user
    
if runtime=="Standard - SQL Only":
    cluster_json["spark_conf"] = {
      "spark.databricks.sql.initial.catalog.name": "hive_metastore",
      "spark.databricks.unityCatalog.enabled": "true",
      "spark.databricks.cluster.profile": "serverless",
      "spark.databricks.repl.allowedLanguages": "sql",
      "spark.databricks.acl.sqlOnly": "true",
      "spark.databricks.acl.dfAclsEnabled": "true"        
    }

# declare SQL endpoint config    

endpoint_size = "MEDIUM"
endpoint_json = {
    "name": cluster_name,
    "size": endpoint_size,
    "max_num_clusters":1,
    "enable_photon": "true",         
    "test_overrides": {"runtime_version": image},
    "conf_pairs":
    {
        "spark.databricks.sql.initial.catalog.name": "hive_metastore",
        "spark.databricks.unityCatalog.enabled": "true",
        "spark.databricks.cluster.profile": "serverless",
        "spark.databricks.repl.allowedLanguages": "sql",
        "spark.databricks.acl.sqlOnly": "true",
        "spark.databricks.acl.dfAclsEnabled": "true"
    },
    "enable_serverless_compute": "false" # DBSQL serverless does not support UC images yet
}

# COMMAND ----------

# create a UC-enabled cluster, or endpoint, depending on the selection

if runtime=="SQL Endpoint":
    response = requests.post(
        host + '/api/2.0/sql/endpoints/',
        headers={"Authorization": "Bearer " + token},
        json = endpoint_json)
    if response.status_code == 200:
        endpoint_id = response.json()['id']
    else:
        raise Exception(f'Error: {response.status_code} {response.reason}')    
else:
    response = requests.post(
      host + '/api/2.0/clusters/create/',
      headers={"Authorization": "Bearer " + token},
      json = cluster_json)
    if response.status_code == 200:
        cluster_id = response.json()['cluster_id']
    else:
        raise Exception(f'Error: {response.status_code} {response.reason}')

while True:
    if runtime=="SQL Endpoint":
        response = requests.get(
          host + '/api/2.0/sql/endpoints/' + endpoint_id,
          headers={"Authorization": "Bearer " + token})
    else:
        response = requests.get(
            host + '/api/2.0/clusters/get',
            headers={"Authorization": "Bearer " + token},
            params={"cluster_id":cluster_id})
    status = response.json()['state']
    
    # waiting until the cluster status changes
    if status not in ["STARTING", "PENDING"]:
        break      
    time.sleep(20)
    print("Waiting for the cluster to start")
    
if runtime=="SQL Endpoint":        
    print(f"UC-enabled SQL endpoint {endpoint_id} status is {status}")
else:
    print(f"UC-enabled cluster {cluster_id} status is {status}")

# COMMAND ----------

if runtime=="SQL Endpoint":
    displayHTML(f"<a href='sql/endpoints/{endpoint_id}'>Link to endpoint</a>")
else:
    displayHTML(f"<a href='#setting/clusters/{cluster_id}/configuration'>Link to cluster</a>")    
