# Databricks notebook source
# MAGIC %pip install azure

# COMMAND ----------

# MAGIC %pip install azure-storage-blob --upgrade

# COMMAND ----------

# MAGIC %pip install azure-storage-file-datalake --pre

# COMMAND ----------

from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from azure.storage.blob import BlobServiceClient
from azure.storage.filedatalake import DataLakeServiceClient

# COMMAND ----------

# # Data Lake setting
storageAccount = "johnlabstorageac" #for manual run
storageContainer = "test" #for manual run
secretScope = 'unity_test_key' #for manual

keyvaultSecret = 'BlobStorageAccessKey'

storageAccountAccessKey = dbutils.secrets.get(scope=secretScope, key=keyvaultSecret)

# Config setting
spark.conf.set('fs.azure.account.key.'+storageAccount+'.dfs.core.windows.net', storageAccountAccessKey)
spark.conf.set('parquet.enable.summary-metadata', 'true')
spark.conf.set('spark.sql.sources.commitProtocolClass', 'org.apache.spark.sql.execution.datasources.SQLHadoopMapReduceCommitProtocol')
spark.conf.set('mapreduce.fileoutputcommitter.marksuccessfuljobs', 'false')

# Create the blob client
connectionString = 'DefaultEndpointsProtocol=https;AccountName='+storageAccount+';AccountKey='+storageAccountAccessKey+';EndpointSuffix=core.windows.net'
blob_service_client = BlobServiceClient.from_connection_string(connectionString)
# Create the container client
container_client = blob_service_client.get_container_client(storageContainer)

# COMMAND ----------


