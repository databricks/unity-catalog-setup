# Databricks notebook source
# MAGIC %md
# MAGIC # UC Metastore setup
# MAGIC 
# MAGIC This is based on details in "Unity Catalog Setup Guide"

# COMMAND ----------

# MAGIC %md
# MAGIC ## READ ME FIRST
# MAGIC - Make sure you are running this notebook as an **Account Administrator** (role need to be set at account level at https://accounts.cloud.databricks.com/)
# MAGIC - Double check the UC special images on Cmd 7
# MAGIC - Unity Catalog set up requires the Databricks CLI with Unity Catalog extension. This is downloaded from Databricks public GDrive link

# COMMAND ----------

# MAGIC %md
# MAGIC ## Import necessary libraries, set input variables for metastore name, bucket location, IAM role, DAC name

# COMMAND ----------

import uuid
import requests
from typing import List
import subprocess
import json

# COMMAND ----------

dbutils.widgets.text("metastore", "unity-catalog")
dbutils.widgets.text("bucket", <bucket>)
dbutils.widgets.text("iam_role", <iam_role>)
dbutils.widgets.text("dac_name", "dac_main")
dbutils.widgets.text("workspace_id", "896752397158209")
dbutils.widgets.text("metastore_admin_users", "metastore-admin-users")

# COMMAND ----------

metastore = dbutils.widgets.get("metastore")
bucket = dbutils.widgets.get("bucket")
iam_role = dbutils.widgets.get("iam_role")
dac_name = dbutils.widgets.get("dac_name")
workspace_id = dbutils.widgets.get("workspace_id")
metastore_admin = dbutils.widgets.get("metastore_admin_users")

# COMMAND ----------

#### DOUBLE-CHECK THE CLUSTER IMAGES #####
spark_version = "custom:custom-local__9.x-snapshot-scala2.12__unknown__head__dc3efb4__12ddf9b__yuchen.huo__0a7f5ed__format-2.lz4"
sql_photon_version = "custom:custom-local__9.x-snapshot-photon-scala2.12__unknown__head__dc3efb4__12ddf9b__yuchen.huo__87f7ac2__format-2.lz4"

# COMMAND ----------

#### Check the databricks-cli-uc gdrive link ####
databricks_cli_gdrive = "14d0du3rENjwqWfGcIcNFmnx7Fz3FRcLg"

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Retrieve Databricks host & token and check if the user is an admin

# COMMAND ----------

host = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().getOrElse(None)
token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().getOrElse(None)
user = dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().get("user").getOrElse(None)

# COMMAND ----------

import requests
import json

# check if the user is admin
response = requests.get(
  host + '/api/2.0/preview/scim/v2/Me',
  headers={"Authorization": "Bearer " + token},
)
if response.status_code == 200:
  is_admin = any([group['display']=='admins' for group in response.json()['groups']])
  if not is_admin:
    raise Exception(f'Not an admin')
else:
  raise Exception(f'Error: {response.status_code} {response.reason}')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create databricks-cli authentication file

# COMMAND ----------

# write the host & tokens out to ~/.databrickscfg for databricks-cli authentication
dbutils.fs.put("file:/root/.databrickscfg",f"""
[DEFAULT]
host = {host}
token = {token}
""", True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Download special databricks-cli and install
# MAGIC **Note:** the below cell downloads from Google Drive, so will only work if Internet access is allowed on the workspace and GDrive is not blocked
# MAGIC 
# MAGIC Alternatively, download the cli to a local machine, and upload it to a dbfs location, then use dbutils.fs.cp to move it to `/tmp/databricks_cli.tgz` on the driver 

# COMMAND ----------

import requests

def download_file_from_google_drive(id, destination):
    URL = "https://docs.google.com/uc?export=download"

    session = requests.Session()

    response = session.get(URL, params = { 'id' : id }, stream = True)
    token = get_confirm_token(response)

    if token:
        params = { 'id' : id, 'confirm' : token }
        response = session.get(URL, params = params, stream = True)

    save_response_content(response, destination)    

def get_confirm_token(response):
    for key, value in response.cookies.items():
        if key.startswith('download_warning'):
            return value

    return None

def save_response_content(response, destination):
    CHUNK_SIZE = 32768

    with open(destination, "wb") as f:
        for chunk in response.iter_content(CHUNK_SIZE):
            if chunk: # filter out keep-alive new chunks
                f.write(chunk)
                
                
download_file_from_google_drive(databricks_cli_gdrive, "/tmp/databricks_cli.tgz")

# COMMAND ----------

# MAGIC %sh
# MAGIC # install databricks-cli-uc
# MAGIC cd /tmp/
# MAGIC tar xzf databricks_cli.tgz
# MAGIC cd /tmp/databricks-cli-uc
# MAGIC virtualenv venv
# MAGIC . venv/bin/activate
# MAGIC pip install -e .

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define a helper function to run dbcli uc command

# COMMAND ----------

from typing import List
import subprocess

# helper function to execute db-cli uc commands
def execute_uc(args:List[str]) -> str:
  process = subprocess.run(['/tmp/databricks-cli-uc/venv/bin/databricks', 'unity-catalog'] + args,
              stdout=subprocess.PIPE,
              stderr=subprocess.PIPE,
              universal_newlines=True)
  if process.stderr != "":
    raise Exception(process.stderr)
  if "error" in process.stdout.lower():
    raise Exception(process.stdout)
  return process.stdout

# helper function to execute db-cli commands (for cluster creation)
def execute_dbcli(args:List[str]) -> str:
  process = subprocess.run(['/tmp/databricks-cli-uc/venv/bin/databricks'] + args,
              stdout=subprocess.PIPE,
              stderr=subprocess.PIPE,
              universal_newlines=True)
  if process.stderr != "":
    raise Exception(process.stderr)
  if "error" in process.stdout.lower():
    raise Exception(process.stdout)
  return process.stdout

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create the Unity Catalog metastore

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create the account-level metastore
# MAGIC **Note:** Each Databricks account only requires 1 metastore to be created, so the following command will throw an error when running on a workspace where the account-level metastore already exists

# COMMAND ----------

# Create a Metastore, and store its ID
metastore_id = execute_uc(['create-metastore', '--name', metastore, '--storage-root', bucket])
metastore_id = json.loads(metastore_id)["metastore_id"]
print(f"Metastore {metastore_id} has been set up")

# COMMAND ----------

# For workspaces where account-level metastore already exists, cmd 21 would fail

# metastore_id = "66b5fa0c-adb2-4e47-be71-770ee996a290"

# COMMAND ----------

# Verify the metastore is correctly created and configured
print(f"Metastore summary: \n {execute_uc(['get-metastore', '--id', metastore_id])}")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Assign the metastore to the current workspace
# MAGIC 
# MAGIC This command prints no output for successful run

# COMMAND ----------

print(execute_uc(['assign-metastore', '--metastore-id', metastore_id, '--workspace-id', workspace_id]))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Data access configuration
# MAGIC **Note** This would fail if a DAC with the same name already exists (e.g. from previous set-up)

# COMMAND ----------

# create a DAC named $DAC_NAME, and store its ID
dac_id = execute_uc(['create-dac', '--metastore-id', metastore_id, '--json', f'{{"name": "{dac_name}", "aws_iam_role": {{"role_arn": "{iam_role}"}}}}'])
dac_id = json.loads(dac_id)["id"]
print(f"Data access configuration {dac_id} has been set up")

# COMMAND ----------

# Verify the data access configuration
print(f"Data access configuration: \n {execute_uc(['get-dac', '--metastore-id', metastore_id, '--dac-id', dac_id])}")

# COMMAND ----------

# Configure the metastore with this DAC
print(execute_uc(['update-metastore', '--id', metastore_id, '--json', f'{{"default_data_access_config_id":"{dac_id}"}}']))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Verification

# COMMAND ----------

# Verify the current metastore
print(f"Current metastore setup: \n {execute_uc(['metastore-summary'])}")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Set metastore permission

# COMMAND ----------

# Update the metastore owner to metastore admin group
print(execute_uc(['update-metastore', '--id', metastore_id, '--json', f'{{"owner":"{metastore_admin}"}}']))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Set catalog permission

# COMMAND ----------

# Grant full access to main catalog for metastore admin group
print(execute_uc(['update-permissions', '--catalog', 'main', '--json', f'{{"changes": [{{"principal": "{metastore_admin}","add": ["CREATE","USAGE"]}}]}}']))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Enable Delta sharing

# COMMAND ----------

# Enable Delta Sharing on the metastore
print(execute_uc(['update-metastore', '--id', metastore_id, '--json', '{"delta_sharing_enabled":true}']))

# COMMAND ----------

# Validate that Delta Sharing is enabled
delta_sharing = execute_uc(['get-metastore', '--id', metastore_id])
delta_sharing = json.loads(delta_sharing)["delta_sharing_enabled"]

print(f"Delta Sharing is {'enabled' if delta_sharing else 'disabled'}")

# COMMAND ----------

# MAGIC  %md
# MAGIC  ## Create a UC-enabled cluster

# COMMAND ----------

import uuid

cluster_json = {
    "num_workers": 1,
    "cluster_name": "uc-cluster-" + uuid.uuid4().hex[:8],
    "spark_version": spark_version,
    "spark_conf": {
        "spark.databricks.sql.initial.catalog.namespace": "main",
        "spark.databricks.managedCatalog": "com.databricks.sql.managedcatalog.PermissionEnforcingManagedCatalog",
        "spark.databricks.unityCatalog.enabled": "true"
    },
    "aws_attributes": {
      "availability": "SPOT"
    },  
    "spark_env_vars": {
        "PYSPARK_PYTHON": "/databricks/python3/bin/python3"
    },  
    "node_type_id": "i3.xlarge",
    "driver_node_type_id": "i3.xlarge",
    "autotermination_minutes": 120,
    "enable_elastic_disk": False,
}

# COMMAND ----------

# create a UC-enabled cluster - this will check until the cluster is not in PENDING state

cluster_id = execute_dbcli(['clusters', 'create', '--json', json.dumps(cluster_json)])
cluster_id = json.loads(cluster_id)["cluster_id"]
import time
while True:
  status = json.loads(execute_dbcli(['clusters', 'get', '--cluster-id', cluster_id]))["state"]
  time.sleep(20)
  print("Waiting for cluster to start")
  if status != "PENDING":
    break
print(f"UC-enabled cluster {cluster_id} status is {status}")

# COMMAND ----------

displayHTML(f"<a href='#setting/clusters/{cluster_id}/configuration'>Link to cluster</a>")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create a UC-enabled SQL endpoint

# COMMAND ----------

# create a UC-enabled SQL endpoint - check for progress in the SQL endpoints screen

endpoint_name = "uc-endpoint-" + uuid.uuid4().hex[:8]
endpoint_size = "MEDIUM"

post_body = {
  "name":endpoint_name,
  "size":endpoint_size,
  "max_num_clusters":1,
  "enable_photon": "true",         
  "test_overrides": {"runtime_version": sql_photon_version},
  "conf_pairs":
  {"spark.databricks.managedCatalog":"com.databricks.sql.managedcatalog.PermissionEnforcingManagedCatalog",
  "spark.databricks.sql.initial.catalog.namespace":"main",
  "spark.databricks.unityCatalog.enabled":"true"},
  "enable_databricks_compute": "false"}

response = requests.post(
  host + '/api/2.0/sql/endpoints/',
  headers={"Authorization": "Bearer " + token},
  json = post_body
)

if response.status_code == 200:
  print(response.json())
else:
  raise Exception(f'Error: {response.status_code} {response.reason}')

# COMMAND ----------

endpoint_id = response.json()["id"]
displayHTML(f"<a href='sql/endpoints/{endpoint_id}'>Link to endpoint</a>")