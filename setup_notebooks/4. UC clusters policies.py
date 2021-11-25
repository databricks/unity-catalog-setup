# Databricks notebook source
import requests
import json

# COMMAND ----------

host = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().getOrElse(None)
token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().getOrElse(None)

# COMMAND ----------

sql_policy_json = {
    "name": "uc-sql-only",
    "definition": "{\n  \"spark_version\": {\n    \"type\": \"regex\",\n    \"pattern\": \"10\\\\.[1-9]*\\\\.x-scala.*\"\n  },\n  \"spark_conf.spark.databricks.unityCatalog.enabled\": {\n    \"type\": \"fixed\",\n    \"value\": \"true\",\n    \"hidden\": true\n  },\n  \"spark_conf.spark.databricks.repl.allowedLanguages\": {\n    \"type\": \"fixed\",\n    \"value\": \"sql\"\n  },\n  \"spark_conf.spark.databricks.cluster.profile\": {\n    \"type\": \"fixed\",\n    \"value\": \"serverless\"\n  },\n  \"spark_conf.spark.databricks.acl.sqlOnly\": {\n    \"type\": \"fixed\",\n    \"value\": \"true\"\n  },\n  \"spark_conf.spark.databricks.acl.dfAclsEnabled\": {\n    \"type\": \"fixed\",\n    \"value\": \"true\"\n  },\n  \"spark_conf.spark.databricks.sql.initial.catalog.name\": {\n    \"type\": \"fixed\",\n    \"value\": \"hive_metastore\"\n  }\n}"
}

multi_policy_json = {
    "name": "uc-multi-lang",
    "definition": "{\n  \"spark_version\": {\n    \"type\": \"regex\",\n    \"pattern\": \"10\\\\.[1-9]*\\\\.x.*-scala.*\"\n  },\n  \"spark_conf.spark.databricks.unityCatalog.enabled\": {\n    \"type\": \"fixed\",\n    \"value\": \"true\",\n    \"hidden\": true\n  },\n  \"spark_conf.spark.databricks.unityCatalog.enforce.permissions\": {\n    \"type\": \"fixed\",\n    \"value\": \"false\",\n    \"hidden\": true\n  },\n  \"spark_conf.spark.databricks.passthrough.enabled\": {\n    \"type\": \"fixed\",\n    \"value\": \"true\",\n    \"hidden\": true\n  }\n}"
}

# COMMAND ----------

response = requests.post(
  host + '/api/2.0/policies/clusters/create',
  headers={"Authorization": "Bearer " + token},
  json = sql_policy_json)
if response.status_code == 200:
    print(response.json())
else:
    raise Exception(f'Error: {response.status_code} {response.json()}')
    
response = requests.post(
  host + '/api/2.0/policies/clusters/create',
  headers={"Authorization": "Bearer " + token},
  json = multi_policy_json)
if response.status_code == 200:
    print(response.json())
else:
    raise Exception(f'Error: {response.status_code} {response.json()}')    
