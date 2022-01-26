# Databricks notebook source
# MAGIC %md
# MAGIC ### Example audit logs ETL notebook
# MAGIC 
# MAGIC - Need to run on a single-user cluster, DBR 10.2+
# MAGIC - Require an instance profile for access to the log bucket (where audit logs are delivered), and the sink bucket (location for AutoLoader schema and streaming checkpoints)

# COMMAND ----------

catalog = dbutils.widgets.get("catalog")
database = dbutils.widgets.get("database")
log_bucket = dbutils.widgets.get("log_bucket")
sink_bucket = dbutils.widgets.get("sink_bucket").strip("/")

# COMMAND ----------

from pyspark.sql.functions import udf, col, from_unixtime, from_utc_timestamp, from_json
from pyspark.sql.types import StringType, StructField, StructType
import json, time

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
spark.sql(f"CREATE DATABASE IF NOT EXISTS {catalog}.{database}")

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

bronze_table = f"{catalog}.{database}.bronze"
checkpoint_path = f"{sink_bucket}/checkpoints"

(streamDF
    .writeStream
    .format("delta")
    .partitionBy("date")
    .outputMode("append")
    .option("checkpointLocation", f"{checkpoint_path}/bronze") 
    .option("cloudFiles.maxBytesPerTrigger", "1g")
    .option("mergeSchema", True)
    .trigger(availableNow=True)
    .toTable(bronze_table)
)

# COMMAND ----------

while spark.streams.active != []:
    print("Waiting for streaming query to finish.")
    time.sleep(5)

# COMMAND ----------

spark.sql(f"OPTIMIZE {bronze_table}")

# COMMAND ----------

def stripNulls(raw):
    return json.dumps({i: raw.asDict()[i] for i in raw.asDict() if raw.asDict()[i] != None})
strip_udf = udf(stripNulls, StringType())

# COMMAND ----------

bronzeDF = spark.readStream.table(bronze_table)

query = (
    bronzeDF
    .withColumn("flattened", strip_udf("requestParams"))
    .withColumn("email", col("userIdentity.email"))
    .withColumn("date_time", from_utc_timestamp(from_unixtime(col("timestamp")/1000), "UTC"))
    .drop("requestParams")
    .drop("userIdentity")
)

# COMMAND ----------

silver_table = f"{catalog}.{database}.silver"

(
    query
    .writeStream
    .format("delta")
    .partitionBy("date")
    .outputMode("append")
    .option("checkpointLocation", f"{checkpoint_path}/silver")
    .option("mergeSchema", True)
    .trigger(availableNow=True)
    .toTable(silver_table)
)

# COMMAND ----------

while spark.streams.active != []:
  print("Waiting for streaming query to finish.")
  time.sleep(5)

# COMMAND ----------

assert(spark.table(bronze_table).count() == spark.table(silver_table).count())

# COMMAND ----------

spark.sql(f"OPTIMIZE {silver_table}")

# COMMAND ----------

@udf(StringType())
def just_keys_udf(string):
    return [i for i in json.loads(string).keys()]

# COMMAND ----------

def flatten_table(service):
    
    service_name = service.replace("-","_")
    
    flattenedStream = spark.readStream.table(silver_table)
    flattened = spark.table(silver_table)
    
    schema = StructType()
    
    keys = (
        flattened
        .filter(col("serviceName") == service_name)
        .select(just_keys_udf(col("flattened")).alias("keys"))
        .distinct()
        .collect()
    )
    
    keysList = [i.asDict()['keys'][1:-1].split(", ") for i in keys]
    
    keysDistinct = {key for keys in keysList for key in keys if key != ""}
    
    if len(keysDistinct) == 0:
        schema.add(StructField('placeholder', StringType()))
    else:
        for key in keysDistinct:
            schema.add(StructField(key, StringType()))
        
    # write the df with the correct schema to table
    (flattenedStream
     .filter(col("serviceName") == service_name)
     .withColumn("requestParams", from_json(col("flattened"), schema))
     .drop("flattened")
     .writeStream
     .partitionBy("date")
     .outputMode("append")
     .format("delta")
     .option("checkpointLocation", f"{checkpoint_path}/gold/{service_name}")
     .option("mergeSchema", True)
     .trigger(availableNow=True)
     .toTable(f"{catalog}.{database}.{service_name}")
    )
    
    # optimize the table as well
    spark.sql(f"OPTIMIZE {catalog}.{database}.{service_name}")

# COMMAND ----------

import threading

#For each table name (i.e. event type) create a separate thread and run the ThreadWorker function to save the data to Delta tables.
threads = [
    threading.Thread(target=flatten_table, args=(service)) 
    for service in spark.table(silver_table).select("serviceName").distinct().collect()
]

for thread in threads:
    thread.start()

for thread in threads:
    thread.join()

# COMMAND ----------

while spark.streams.active != []:
    print("Waiting for streaming query to finish.")
    time.sleep(5)

# COMMAND ----------

display(spark.sql(f"SHOW TABLES IN {catalog}.{database}"))
