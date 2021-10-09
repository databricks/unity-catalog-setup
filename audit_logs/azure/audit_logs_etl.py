# Databricks notebook source
APPLICATION_ID = "ed573937-9c53-4ed6-b016-929e765443eb"
DIRECTORY_ID = "9f37a392-f0ae-4280-9796-f1864a10effc"
APP_KEY = ""

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.vnadls.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.vnadls.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.vnadls.dfs.core.windows.net", APPLICATION_ID)
spark.conf.set("fs.azure.account.oauth2.client.secret.vnadls.dfs.core.windows.net", APP_KEY)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.vnadls.dfs.core.windows.net", f"https://login.microsoftonline.com/{DIRECTORY_ID}/oauth2/token")

# COMMAND ----------

log_bucket = dbutils.widgets.get("log_bucket")
sink_bucket = dbutils.widgets.get("sink_bucket")

# COMMAND ----------

from pyspark.sql.functions import udf, col, from_unixtime, from_utc_timestamp, from_json, expr
from pyspark.sql.types import StringType, StructField, StructType
import json, time

# COMMAND ----------

streamDF = (
  spark
  .readStream
  .format("cloudFiles")
  .option("cloudFiles.format", "json")
  .option("cloudFiles.schemaLocation", f"{sink_bucket}/audit_log_schema")
  .option("cloudFiles.includeExistingFiles", True)
  .option("cloudFiles.inferColumnTypes", True)
  .option("cloudFiles.partitionColumns", "")
  .load(log_bucket)
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
# MAGIC CREATE DATABASE IF NOT EXISTS az_audit_logs

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS az_audit_logs.bronze
USING DELTA
LOCATION '{bronze_path}'
""")

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE az_audit_logs.bronze

# COMMAND ----------

bronzeDF = spark.readStream.load(bronze_path)

properties_columns = ["actionName", "logId", "requestId", "requestParams", "response", "serviceName", "sourceIPAddress", "userAgent", "sessionId"]

for property in properties_columns:
  bronzeDF = bronzeDF.withColumn(property, col(f"properties.{property}"))

query = (
  bronzeDF
  .withColumn("email", expr("identity:email"))
  .withColumn("date_time", from_utc_timestamp(from_unixtime(col("time")/1000), "UTC"))
  .drop("identity", "properties", "_rescued_data")
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
CREATE TABLE IF NOT EXISTS az_audit_logs.silver
USING DELTA
LOCATION '{silver_path}'
""")

# COMMAND ----------

assert(spark.table("az_audit_logs.bronze").count() == spark.table("az_audit_logs.silver").count())

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE az_audit_logs.silver

# COMMAND ----------

def justKeys(string):
  return [i for i in json.loads(string).keys()]
just_keys_udf = udf(justKeys, StringType())

# COMMAND ----------

def flatten_table(service_name, gold_path):
  flattenedStream = spark.readStream.load(silver_path)
  flattened = spark.table("az_audit_logs.silver")
  
  schema = StructType()
  
  keys = (
    flattened
    .filter(col("serviceName") == service_name)
    .select(just_keys_udf(col("requestParams")))
    .alias("keys")
    .distinct()
    .collect()
  )
  
  keysList = [i.asDict()['justKeys(requestParams)'][1:-1].split(", ") for i in keys]
  
  keysDistinct = {j for i in keysList for j in i if j != ""}
  
  if len(keysDistinct) == 0:
    schema.add(StructField('placeholder', StringType()))
  else:
    for i in keysDistinct:
      schema.add(StructField(i, StringType()))
    
  (flattenedStream
   .filter(col("serviceName") == service_name)
   .withColumn("requestParams", from_json(col("requestParams"), schema))
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

service_name_list = [i['serviceName'] for i in spark.table("az_audit_logs.silver").select("serviceName").distinct().collect()]

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
  CREATE TABLE IF NOT EXISTS az_audit_logs.{service_name}
  USING DELTA
  LOCATION '{gold_path}/{service_name}'
  """)
  spark.sql(f"OPTIMIZE az_audit_logs.{service_name}")

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN az_audit_logs
