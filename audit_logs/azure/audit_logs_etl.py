# Databricks notebook source
# MAGIC %md
# MAGIC ###Install Event Hub Spark connector

# COMMAND ----------

cntx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
host = cntx.apiUrl().getOrElse(None)
token = cntx.apiToken().getOrElse(None)
post_body = {
  "cluster_id": cntx.clusterId().getOrElse(None),
  "libraries": [
    {
      "maven": {
        "coordinates": "com.microsoft.azure:azure-eventhubs-spark_2.12:2.3.21"
      }   
    }
  ]
}

# COMMAND ----------

import requests

response = requests.post(
  host + '/api/2.0/libraries/install',
  headers={"Authorization": "Bearer " + token},
  json = post_body
)

if response.status_code == 200:
  print(response.json())
else:
  raise Exception(f'Error: {response.status_code} {response.reason}')

# COMMAND ----------

# wait until library is installed
import json, time

while True:
  response = requests.get(
    host + f'/api/2.0/libraries/cluster-status?cluster_id={cntx.clusterId().getOrElse(None)}',
    headers={"Authorization": "Bearer " + token},
  )
  status = [library['status'] for library in response.json()['library_statuses'] if 'eventhubs-spark' in json.dumps(library['library'])][0]
  if status != "INSTALLING":
    break
  time.sleep(2)
  print("Waiting for library to install")

# COMMAND ----------

APPLICATION_ID = "ed573937-9c53-4ed6-b016-929e765443eb"
DIRECTORY_ID = "9f37a392-f0ae-4280-9796-f1864a10effc"
APP_KEY = "xxx"

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.vnadls.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.vnadls.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.vnadls.dfs.core.windows.net", APPLICATION_ID)
spark.conf.set("fs.azure.account.oauth2.client.secret.vnadls.dfs.core.windows.net", APP_KEY)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.vnadls.dfs.core.windows.net", f"https://login.microsoftonline.com/{DIRECTORY_ID}/oauth2/token")

# COMMAND ----------

# sink_bucket = dbutils.widgets.get("sink_bucket")
sink_bucket = "abfss://unity-catalog@vnadls.dfs.core.windows.net/"

# COMMAND ----------

connectionString = dbutils.secrets.get("demo", "shared-eventhub-conn")
eventhub_name = 'vn-audit-logs'

ehConf = {}
ehConf['eventhubs.connectionString'] = sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(connectionString)
startingEventPosition = {
  "offset": "-1",  
  "seqNo": -1,            #not in use
  "enqueuedTime": None,   #not in use
  "isInclusive": True
}
ehConf["eventhubs.startingPosition"] = json.dumps(startingEventPosition)

# COMMAND ----------

from pyspark.sql.functions import schema_of_json, lit

log_schema =spark.range(1) \
    .select(schema_of_json(lit("""{
    "records": [
        {
            "resourceId": "/SUBSCRIPTIONS/3F2E4D32-8E8D-46D6-82BC-5BB8D962328B/RESOURCEGROUPS/VN-SANDBOX/PROVIDERS/MICROSOFT.DATABRICKS/WORKSPACES/VN-DEMO-WORKSPACE",
            "operationVersion": "1.0.0",
            "identity": {"email":"vuong.nguyen@databricks.com","subjectName":null},
            "operationName": "Microsoft.Databricks/accounts/aadBrowserLogin",
            "time": "2021-10-06T09:06:41Z",
            "category": "accounts",
            "properties": {
                "sourceIPAddress": "18.193.11.166:0",
                "logId": "c4b0ab3e-cd1e-3952-97ea-c35e37b7b271",
                "serviceName": "accounts",
                "userAgent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.61 Safari/537.36",
                "response": {"statusCode":200},
                "sessionId": null,
                "actionName": "aadBrowserLogin",
                "requestId": "6f05d6d6-c457-477c-9692-0addaa1c1797",
                "requestParams": {"user":"vuong.nguyen@databricks.com"}
            },
            "FluentdIngestTimestamp": "2021-10-06T09:08:05.0000000Z",
            "Host": "1005-155304-lag215-10-139-64-14"
        }
    ]
}"""))) \
    .collect()[0][0]

# COMMAND ----------

from pyspark.sql.functions import col, from_json, regexp_replace, explode, from_unixtime, from_utc_timestamp, udf
import json, time

streamDF = (spark
            .readStream
            .format('eventhubs')
            .options(**ehConf)
            .load()
            .withColumn('payload', col('body').cast(StringType()))
            .withColumn('payload', regexp_replace('payload', '\\\\"', '"'))
            .withColumn('payload', regexp_replace('payload', '"\{', '\{'))
            .withColumn('payload', regexp_replace('payload', '\}"', '\}'))             
            .withColumn('json',from_json(col('payload'), log_schema))
            .select(col('json.*'))
            .select(explode('records'))
            .select(col('col.*'))
            .withColumn('date',col('time').cast('date'))
           )

# COMMAND ----------

bronze_path = f"{sink_bucket}/audit_logs_streaming/bronze"
checkpoint_path = f"{sink_bucket}/checkpoints"

(streamDF
 .writeStream
 .format("delta")
 .partitionBy("date")
 .outputMode("append")
 .option("checkpointLocation", f"{checkpoint_path}/bronze")
 .option("path", bronze_path) 
 .option("mergeSchema", True)
 .trigger(once=True)
 .start()
)

# COMMAND ----------

while spark.streams.active != []:
  print("Waiting for streaming query to finish.")
  time.sleep(5)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS audit_logs

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS audit_logs.bronze
USING DELTA
LOCATION '{bronze_path}'
""")

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE audit_logs.bronze

# COMMAND ----------

def stripNulls(raw):
  return json.dumps({i: raw.asDict()[i] for i in raw.asDict() if raw.asDict()[i] != None})
strip_udf = udf(stripNulls, StringType())

# COMMAND ----------

bronzeDF = spark.readStream.load(bronze_path)

query = (
  bronzeDF
  .withColumn("flattened", strip_udf("properties.requestParams"))
  .withColumn("email", col("identity.email"))
  .withColumn("date_time", from_utc_timestamp(from_unixtime(col("time")/1000), "UTC"))
  .drop("identity")
)

# COMMAND ----------

silver_path = f"{sink_bucket}/audit_logs_streaming/silver"

(query
 .writeStream
 .format("delta")
 .partitionBy("date")
 .outputMode("append")
 .option("checkpointLocation", f"{checkpoint_path}/silver")
 .option("path", silver_path)
 .option("mergeSchema", True)
 .trigger(once=True)
 .start()
)

# COMMAND ----------

while spark.streams.active != []:
  print("Waiting for streaming query to finish.")
  time.sleep(5)

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS audit_logs.silver
USING DELTA
LOCATION '{silver_path}'
""")

# COMMAND ----------

assert(spark.table("audit_logs.bronze").count() == spark.table("audit_logs.silver").count())

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE audit_logs.silver

# COMMAND ----------

def justKeys(string):
  return [i for i in json.loads(string).keys()]
just_keys_udf = udf(justKeys, StringType())

# COMMAND ----------

def flatten_table(service_name, gold_path):
  flattenedStream = spark.readStream.load(silver_path)
  flattened = spark.table("audit_logs.silver")
  
  schema = StructType()
  
  keys = (
    flattened
    .filter(col("properties.serviceName") == service_name)
    .select(just_keys_udf(col("flattened")))
    .alias("keys")
    .distinct()
    .collect()
  )
  
  keysList = [i.asDict()['justKeys(flattened)'][1:-1].split(", ") for i in keys]
  
  keysDistinct = {j for i in keysList for j in i if j != ""}
  
  if len(keysDistinct) == 0:
    schema.add(StructField('placeholder', StringType()))
  else:
    for i in keysDistinct:
      schema.add(StructField(i, StringType()))
    
  (flattenedStream
   .filter(col("properties.serviceName") == service_name)
   .withColumn("properties.requestParams", from_json(col("flattened"), schema))
   .drop("flattened")
   .writeStream
   .partitionBy("date")
   .outputMode("append")
   .format("delta")
   .option("checkpointLocation", f"{checkpoint_path}/gold/{service_name}")
   .option("path", f"{gold_path}/{service_name}")
   .option("mergeSchema", True)
   .trigger(once=True)
   .start()
  )

# COMMAND ----------

service_name_list = [i['serviceName'] for i in spark.table("audit_logs.silver").select("properties.serviceName").distinct().collect()]

# COMMAND ----------

gold_path = f"{sink_bucket}/audit_logs_streaming/gold"

for service_name in service_name_list:
  flatten_table(service_name, gold_path)

# COMMAND ----------

while spark.streams.active != []:
  print("Waiting for streaming query to finish.")
  time.sleep(5)

# COMMAND ----------

for service_name in service_name_list:
  spark.sql(f"""
  CREATE TABLE IF NOT EXISTS audit_logs.{service_name}
  USING DELTA
  LOCATION '{gold_path}/{service_name}'
  """)
  spark.sql(f"OPTIMIZE audit_logs.{service_name}")

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN audit_logs
