# Databricks notebook source
# MAGIC %sql
# MAGIC -- this is only required if running on a UC-enabled cluster
# MAGIC --USE CATALOG hive_metastore

# COMMAND ----------

database = dbutils.widgets.get("database")
log_bucket = dbutils.widgets.get("log_bucket")
sink_bucket = dbutils.widgets.get("sink_bucket").strip("/")

# COMMAND ----------

from pyspark.sql.functions import udf, col, from_unixtime, from_utc_timestamp, from_json
from pyspark.sql.types import StringType, StructField, StructType
import json, time

# COMMAND ----------

streamDF = (
  spark
  .readStream
  .format("cloudFiles")
  .option("cloudFiles.format", "json")
  .option("cloudFiles.inferColumnTypes", True)
  .option("cloudFiles.schemaHints", "workspaceId long")  
  .option("cloudFiles.schemaLocation", f"{sink_bucket}/audit_log_schema")
  .load(f"{log_bucket}/audit-logs")
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

spark.sql(f"CREATE DATABASE IF NOT EXISTS {database}")

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {database}.bronze
USING DELTA
LOCATION '{bronze_path}'
""")

# COMMAND ----------

spark.sql(f"OPTIMIZE {database}.bronze")

# COMMAND ----------

def stripNulls(raw):
  return json.dumps({i: raw.asDict()[i] for i in raw.asDict() if raw.asDict()[i] != None})
strip_udf = udf(stripNulls, StringType())

# COMMAND ----------

bronzeDF = spark.readStream.load(bronze_path)

query = (
  bronzeDF
  .withColumn("flattened", strip_udf("requestParams"))
  .withColumn("email", col("userIdentity.email"))
  .withColumn("date_time", from_utc_timestamp(from_unixtime(col("timestamp")/1000), "UTC"))
  .drop("requestParams")
  .drop("userIdentity")
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
CREATE TABLE IF NOT EXISTS {database}.silver
USING DELTA
LOCATION '{silver_path}'
""")

# COMMAND ----------

assert(spark.table(f"{database}.bronze").count() == spark.table(f"{database}.silver").count())

# COMMAND ----------

spark.sql(f"OPTIMIZE {database}.silver")

# COMMAND ----------

def justKeys(string):
  return [i for i in json.loads(string).keys()]
just_keys_udf = udf(justKeys, StringType())

# COMMAND ----------

def flatten_table(service_name, gold_path):
  flattenedStream = spark.readStream.load(silver_path)
  flattened = spark.table(f"{database}.silver")
  
  schema = StructType()
  
  keys = (
    flattened
    .filter(col("serviceName") == service_name)
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
   .filter(col("serviceName") == service_name)
   .withColumn("requestParams", from_json(col("flattened"), schema))
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

service_name_list = [i['serviceName'] for i in spark.table(f"{database}.silver").select("serviceName").distinct().collect()]

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
  CREATE TABLE IF NOT EXISTS {database}.{service_name}
  USING DELTA
  LOCATION '{gold_path}/{service_name}'
  """)
  spark.sql(f"OPTIMIZE {database}.{service_name}")

# COMMAND ----------

display(spark.sql(f"SHOW TABLES IN {database}"))
