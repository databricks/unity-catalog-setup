# Databricks notebook source
# MAGIC %md
# MAGIC # UC Metastore Setup
# MAGIC 
# MAGIC This is based on details in "Unity Catalog Setup Guide"

# COMMAND ----------

# MAGIC %md
# MAGIC ## READ ME FIRST
# MAGIC - Make sure you are running this notebook as an **Account Administrator** (role need to be set at account level at https://accounts.cloud.databricks.com/)
# MAGIC - Select the cloud (AWS or Azure) after Cmd 6 is run. Fill in the rest of the widgets with the required information
# MAGIC   - `bucket` - the default storage location for managed tables in Unity Catalog
# MAGIC     - **AWS**: s3 path to the bucket `s3://<bucket>`
# MAGIC     - **Azure**: abfs path to the container `abfss://$CONTAINER_NAME@$STORAGE_ACCOUNT_NAME.dfs.core.windows.net/`
# MAGIC   - `dac_name` - unique name for the Data Access Configuration
# MAGIC   - Credential:
# MAGIC     - **AWS**: `iam_role` - the IAM role to be used by Unity Catalog (`arn:aws:iam::<account_id>:role/<role_name>`)
# MAGIC     - **Azure**:
# MAGIC         - `directory_id` - the directory id of the Azure AD tenant
# MAGIC         - `application_id` - the application id of the service principal
# MAGIC         - `client_secret` - the client secret of the service principal
# MAGIC   - `metastore` - unique name for the metastore
# MAGIC   - `metastore_admin_group` - account-level group who will be the metastore admins
# MAGIC - Double check the UC special images on Cmd 10
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

dbutils.widgets.removeAll()

# COMMAND ----------

dbutils.widgets.dropdown("cloud", "Select one", ["Select one", "AWS", "Azure"])

# COMMAND ----------

cloud = dbutils.widgets.get("cloud")
if cloud == "Select one":
  raise Exception("Need to select a cloud")
  
if cloud == "AWS":
  dbutils.widgets.text("bucket", "s3://bucket")
  dbutils.widgets.text("iam_role", "arn:aws:iam::997819012307:role/role")
elif cloud == "Azure":
  dbutils.widgets.text("bucket", "abfss://$CONTAINER_NAME@$STORAGE_ACCOUNT_NAME.dfs.core.windows.net/")
  dbutils.widgets.text("directory_id", "9f37a392-f0ae-4280-9796-f1864a10effc")
  dbutils.widgets.text("application_id", "ed573937-9c53-4ed6-b016-929e765443eb")
  dbutils.widgets.text("client_secret", "xxxxx")
dbutils.widgets.text("metastore", "unity-catalog")
dbutils.widgets.text("dac_name", "default-dac")
dbutils.widgets.text("metastore_admin_group", "metastore-admin-users")

# COMMAND ----------

if cloud == "AWS":
  iam_role = dbutils.widgets.get("iam_role")
elif cloud == "Azure":
  directory_id = dbutils.widgets.get("directory_id")
  application_id = dbutils.widgets.get("application_id")
  client_secret = dbutils.widgets.get("client_secret")
  
bucket = dbutils.widgets.get("bucket")
metastore = dbutils.widgets.get("metastore")
dac_name = dbutils.widgets.get("dac_name")
metastore_admin = dbutils.widgets.get("metastore_admin_group")

# COMMAND ----------

# format validation of bucket path & iam role

import re

s3_regex = "^s3:\/\/[a-z0-9\-]{3,63}$"
iam_role_regex = "^arn:aws:iam::\d{12}:role/.+"
abfs_regex = "^abfss:\/\/.+\.dfs\.core\.windows\.net(\/)?$"

if cloud == "AWS":
  if not re.match(s3_regex, bucket):
    raise Exception("Not a valid s3 path")

  if not re.match(iam_role_regex, iam_role):
    raise Exception("Not a valid IAM role arn")
    
elif cloud == "Azure":
  if not re.match(abfs_regex, bucket):
    raise Exception("Not a valid abfs path")  

# COMMAND ----------

#### DOUBLE-CHECK THE CLUSTER IMAGES #####
spark_version = "custom:custom-local__9.x-snapshot-scala2.12__unknown__master__ed485b9__7d4fea6__lin.zhou__992a059__format-2.lz4"
sql_photon_version = "custom:custom-local__9.x-snapshot-photon-scala2.12__unknown__head__dfac5b8__62428b8__yuchen.huo__57c2e42__format-2.lz4"

# COMMAND ----------

#### Check the databricks-cli-uc gdrive link ####
databricks_cli_gdrive = "18Zyfyo8kCRSFp-ISHw_hj3oBGK_TTBz7"

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Retrieve Databricks host & token and check if the user is an admin

# COMMAND ----------

host = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().getOrElse(None)
token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().getOrElse(None)
user = dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().get("user").getOrElse(None)
workspace_id = dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().get("orgId").getOrElse(None)

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
# MAGIC ## Create the Unity Catalog metastore (only once per Databricks account)
# MAGIC **Note:** Each Databricks account only requires 1 metastore to be created, so the following command will throw an error when running on a workspace where the account-level metastore already exists
# MAGIC 
# MAGIC **Skip to Cmd 29 if that is the case **

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create the account-level metastore

# COMMAND ----------

# Create a Metastore, and store its ID
metastore_id = execute_uc(['create-metastore', '--name', metastore, '--storage-root', bucket])
metastore_id = json.loads(metastore_id)["metastore_id"]
print(f"Metastore {metastore_id} has been set up")

# COMMAND ----------

# Verify the metastore is correctly created and configured
print(f"Metastore summary: \n {execute_uc(['get-metastore', '--id', metastore_id])}")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Data access configuration
# MAGIC **Note** This would fail if a DAC with the same name already exists (e.g. from previous set-up)

# COMMAND ----------

# create a DAC named $DAC_NAME, and store its ID
if cloud == "AWS":
  dac_id = execute_uc(['create-dac', '--metastore-id', metastore_id, '--json', f'{{"name": "{dac_name}", "aws_iam_role": {{"role_arn": "{iam_role}"}}}}'])
elif cloud == "Azure":
  dac_id = execute_uc(['create-dac', '--metastore-id', metastore_id, '--json', f'{{"name": "{dac_name}", "azure_service_principal": {{"directory_id": "{directory_id}", "application_id": "{application_id}", "client_secret":"{client_secret}"}}}}'])
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
# MAGIC #### Set metastore permission

# COMMAND ----------

# Update the metastore owner to metastore admin group
print(execute_uc(['update-metastore', '--id', metastore_id, '--json', f'{{"owner":"{metastore_admin}"}}']))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Assign the metastore to the current workspace
# MAGIC 
# MAGIC This command prints no output for successful run

# COMMAND ----------

# For workspaces where account-level metastore already exists, need to specify the metastore_id

# metastore_id = "66b5fa0c-adb2-4e47-be71-770ee996a290"

# COMMAND ----------

print(execute_uc(['assign-metastore', '--metastore-id', metastore_id, '--workspace-id', workspace_id]))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Set catalog permission

# COMMAND ----------

# Grant full access to main catalog for metastore admin group
print(execute_uc(['update-permissions', '--catalog', 'main', '--json', f'{{"changes": [{{"principal": "{metastore_admin}","add": ["CREATE","USAGE"]}}]}}']))

# COMMAND ----------

# Grant full access to main catalog for admin running the notebook as well
print(execute_uc(['update-permissions', '--catalog', 'main', '--json', f'{{"changes": [{{"principal": "{user}","add": ["CREATE","USAGE"]}}]}}']))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Verification

# COMMAND ----------

# Verify the current metastore
print(f"Current metastore setup: \n {execute_uc(['metastore-summary'])}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Enable Delta sharing

# COMMAND ----------

# Enable Delta Sharing on the metastore, delta_sharing_recipient_token_lifetime_in_seconds must be provided, use 0 for infinite lifetime. 31536000 seconds = 1 year
print(execute_uc(['update-metastore', '--id', metastore_id, '--json', '{"delta_sharing_enabled":true, "delta_sharing_recipient_token_lifetime_in_seconds":31536000}']))

# COMMAND ----------

# Validate that Delta Sharing is enabled
delta_sharing = execute_uc(['get-metastore', '--id', metastore_id])
delta_sharing = json.loads(delta_sharing)["delta_sharing_enabled"]

print(f"Delta Sharing is {'enabled' if delta_sharing else 'disabled'}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create a quickstart catalog

# COMMAND ----------

print(execute_uc(['create-catalog', '--name', 'quickstart_catalog']))

# COMMAND ----------

# Grant full access to main catalog for metastore admin group
print(execute_uc(['update-permissions', '--catalog', 'quickstart_catalog', '--json', f'{{"changes": [{{"principal": "{metastore_admin}","add": ["CREATE","USAGE"]}}]}}']))
