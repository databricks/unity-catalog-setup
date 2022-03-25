# Databricks notebook source
import requests

host = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().getOrElse(None)
token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().getOrElse(None)

application_id = <Application (client) ID>
directory_id = <Directory (tenant) ID>
client_secret = <Client secret value>

# COMMAND ----------

metastore_summary = requests.get(
    host + "/api/2.0/unity-catalog/metastore_summary",
    headers = {"Authorization": "Bearer " + token},
)

storage_root_credential_id = metastore_summary.json()['storage_root_credential_id']

storage_credentials = requests.get(
    host + "/api/2.0/unity-catalog/storage-credentials",
    headers = {"Authorization": "Bearer " + token},
)

storage_credential_name = [
    credential['name'] for credential in storage_credentials.json()['storage_credentials'] 
    if credential['id'] == storage_root_credential_id
][0]

# COMMAND ----------

update = requests.post(
    host + "/api/2.0/unity-catalog/storage-credentials",
    headers = {"Authorization": "Bearer " + token},
    data = {
        "name": storage_credential_name,
        "azure_service_principal": {
            "directory_id": directory_id,
            "application_id": application_id,
            "client_secret": client_secret
            }
  }
)
