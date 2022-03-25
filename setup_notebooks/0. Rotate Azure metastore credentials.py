# Databricks notebook source
import requests

host = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().getOrElse(None)
token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().getOrElse(None)

metastore_summary = requests.get(
    host + "/api/2.0/unity-catalog/metastore_summary",
    headers = {"Authorization": "Bearer " + token},
)

storage_root_credential_id = metastore_summary.json()['storage_root_credential_id']

storage_credentials = requests.get(
    host + "/api/2.0/unity-catalog/storage-credentials",
    headers = {"Authorization": "Bearer " + token},
)

root_credential = [
    credential for credential in storage_credentials.json()['storage_credentials'] 
    if credential['id'] == storage_root_credential_id
][0]

storage_credential_name = root_credential['name']
application_id = root_credential['azure_service_principal']['application_id']
directory_id = root_credential['azure_service_principal']['directory_id']
print(f'Please create a new secret for application {application_id} in your AAD directory {directory_id}')

# COMMAND ----------

client_secret = <New client secret value>

update = requests.patch(
    host + f"/api/2.0/unity-catalog/storage-credentials/{storage_credential_name}",
    headers = {"Authorization": "Bearer " + token},
    json = {
        "azure_service_principal": {
            "directory_id": directory_id,
            "application_id": application_id,
            "client_secret": client_secret
            }
  }
)

if update.status_code == 200:
    print(update.json())
else:
    raise Exception(update.json())
