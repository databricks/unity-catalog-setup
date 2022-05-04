# Databricks notebook source
# MAGIC %md
# MAGIC # UC Metastore Setup (legacy)
# MAGIC 
# MAGIC 
# MAGIC *Preferably leverage the Account Console, or the Terraform guide ([AWS](https://registry.terraform.io/providers/databrickslabs/databricks/latest/docs/guides/unity-catalog), [Azure](https://registry.terraform.io/providers/databrickslabs/databricks/latest/docs/guides/unity-catalog-azure))*

# COMMAND ----------

# MAGIC %md
# MAGIC ## READ ME FIRST
# MAGIC - Refer to docs for [AWS](https://docs.databricks.com/data-governance/unity-catalog/get-started.html), [Azure]()
# MAGIC - Make sure you are running this notebook as an **Account Administrator** (need to be set up by Databricks team)
# MAGIC - Fill in the widgets with the required information
# MAGIC   - `bucket` - the default storage location for managed tables in Unity Catalog
# MAGIC     - **Azure**: abfs path to the container `abfss://<CONTAINER_NAME>@<STORAGE_ACCOUNT_NAME>.dfs.core.windows.net/`
# MAGIC   - `storage_credential_name` - unique name for the storage credential
# MAGIC   - Credential:
# MAGIC     - **Azure**:
# MAGIC         - `directory_id` - the directory id of the Azure AD tenant
# MAGIC         - `application_id` - the application id of the service principal
# MAGIC         - `client_secret` - the client secret of the service principal
# MAGIC   - `metastore` - unique name for the metastore
# MAGIC   - `metastore_admin_group` - account-level group who will be the metastore admins (this should be created in Azure AD)
# MAGIC - Unity Catalog set up requires the Databricks CLI with Unity Catalog extension. This is installed from pip

# COMMAND ----------

# MAGIC %md
# MAGIC ## Download special databricks-cli and install from pip

# COMMAND ----------

# MAGIC %pip install databricks-cli-uc

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

dbutils.widgets.text("bucket", "abfss://container-name@storageaccount.dfs.core.windows.net/", "ADLS Container URL")
dbutils.widgets.text("directory_id", "9f37a392-f0ae-4280-9796-f1864a10effc", "Azure Tenant ID")
dbutils.widgets.text("application_id", "ed573937-9c53-4ed6-b016-929e765443eb", "AAD Application ID")
dbutils.widgets.text("client_secret", "xxxxx", "Client Secret")
dbutils.widgets.text("workspace_id",
                     dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().get("orgId").getOrElse(None),
                     "Workspace ID")
dbutils.widgets.text("metastore", "unity-catalog", "UC Metastore Name")
dbutils.widgets.text("storage_credential_name", "default-credential", "UC Storage Credential Name")
dbutils.widgets.text("metastore_admin_group", "metastore-admin-users", "UC Metastore Admin Group")

# COMMAND ----------

directory_id = dbutils.widgets.get("directory_id")
application_id = dbutils.widgets.get("application_id")
client_secret = dbutils.widgets.get("client_secret")
workspace_id = dbutils.widgets.get("workspace_id")
  
bucket = dbutils.widgets.get("bucket")
metastore = dbutils.widgets.get("metastore")
storage_credential_name = dbutils.widgets.get("storage_credential_name")
metastore_admin = dbutils.widgets.get("metastore_admin_group")

# COMMAND ----------

# format validation of bucket path

import re

abfs_regex = "^abfss:\/\/[a-z0-9-]+@[a-z0-9]+\.dfs\.core\.windows\.net(\/)?$"
    
if not re.match(abfs_regex, bucket):
    raise Exception("Not a valid abfs path")  

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Retrieve Databricks host & token

# COMMAND ----------

host = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().getOrElse(None)
token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().getOrElse(None)
user = dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().get("user").getOrElse(None)

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
# MAGIC ## Define a helper function to run dbcli uc command

# COMMAND ----------

from typing import List
import subprocess

# helper function to execute db-cli uc commands
def execute_uc(args:List[str]) -> str:
    process = subprocess.run(['databricks', 'unity-catalog'] + args,
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
# MAGIC ## Create a Unity Catalog metastore

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create the account-level metastore
# MAGIC **Note:** This below command would fail if a metastore with the same name already exists (e.g. from previous set-up). Just pick a different name in that case

# COMMAND ----------

# Create a Metastore, and store its ID
metastore_id = execute_uc(['create-metastore', '--name', metastore, '--storage-root', bucket])
metastore_id = json.loads(metastore_id)["metastore_id"]

# COMMAND ----------

# MAGIC %md
# MAGIC #### Assign the metastore to the current workspace
# MAGIC 
# MAGIC This command prints no output for successful run

# COMMAND ----------

print(execute_uc(['assign-metastore', '--metastore-id', metastore_id, '--workspace-id', workspace_id, '--default-catalog-name', 'hive_metastore']))

# COMMAND ----------

# MAGIC %md
# MAGIC The above command can be repeated for other workspaces to be assigned to this newly created metastore

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create storage credential configuration
# MAGIC **Note:** This would fail if a storage credential with the same name already exists (e.g. from previous set-up). Just pick a different name in that case

# COMMAND ----------

# create a storage credential named $CREDENTIAL_NAME, and store its ID
credential_id = execute_uc(['create-storage-credential', '--json', f'{{"name": "{storage_credential_name}", "azure_service_principal": {{"directory_id": "{directory_id}", "application_id": "{application_id}", "client_secret":"{client_secret}"}}}}', '--debug'])
clean_resp = re.search("chunked(.*)", credential_id.replace('\n',''))
credential_id = json.loads(clean_resp.group(1))["id"]
print(f"Storage credential configuration {credential_id} has been set up")

# COMMAND ----------

# update the metastore with the storage credential
execute_uc(['update-metastore', '--id', metastore_id, '--json', f'{{"storage_root_credential_id": "{credential_id}"}}'])
print(f"Metastore {metastore_id} has been set up")

# COMMAND ----------

# Verify the metastore is correctly created and configured
print(f"Metastore summary: \n {execute_uc(['get-metastore', '--id', metastore_id])}")

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
