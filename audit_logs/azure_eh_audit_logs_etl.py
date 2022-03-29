# Databricks notebook source
import threading
import json
import time
from pyspark.sql.types import *
from pyspark.sql.functions import col, from_json, explode, unix_timestamp
dbutils.widgets.text("catalog", "audit_logs")
dbutils.widgets.text("database", "azure")
dbutils.widgets.text(
    "sink_bucket", "s3://databricks-uc-field-eng-kpwfklmr/unity-azure")

# COMMAND ----------

catalog = dbutils.widgets.get("catalog")
database = dbutils.widgets.get("database")
sink_bucket = dbutils.widgets.get("sink_bucket").strip("/")

# COMMAND ----------


# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
spark.sql(f"CREATE DATABASE IF NOT EXISTS {catalog}.{database}")

# COMMAND ----------

# event hub configuration

connSharedAccessKey = <enter key here >

TOPIC = "unity"
BOOTSTRAP_SERVERS = "fieldengdeveastus2ehb.servicebus.windows.net:9093"
EH_SASL = f'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="$ConnectionString" password="{connSharedAccessKey}";'

# COMMAND ----------

df = (spark.readStream
      .format("kafka")
      .option("subscribe", TOPIC)
      .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
      .option("kafka.sasl.mechanism", "PLAIN")
      .option("kafka.security.protocol", "SASL_SSL")
      .option("kafka.sasl.jaas.config", EH_SASL)
      .option("kafka.request.timeout.ms", "60000")
      .option("kafka.session.timeout.ms", "60000")
      .option("failOnDataLoss", "false")
      .option("startingOffsets", "earliest")
      .option("maxOffsetsPerTrigger", 100000)
      .load()
      .withColumn("deserializedBody", col("value").cast("string"))
      .withColumn("date", col("timestamp").cast("date"))
      .drop("value")
      )

# COMMAND ----------

bronze_table = f"{catalog}.{database}.bronze"
checkpoint_path = f"{sink_bucket}/checkpoints"

(df
    .writeStream
    .format("delta")
    .partitionBy("date")
    .outputMode("append")
    .option("checkpointLocation", f"{checkpoint_path}/bronze")
    .option("mergeSchema", True)
    .trigger(availableNow=True)
    .toTable(bronze_table)
 )

# COMMAND ----------

while spark.streams.active != []:
    print("Waiting for streaming query to finish.")
    time.sleep(5)

spark.sql(f"OPTIMIZE {bronze_table}")

# COMMAND ----------

raw_schema = (StructType([
    StructField("records", ArrayType(StructType([
        StructField("Host", StringType()),
        StructField("category", StringType()),
        StructField("identity", StringType()),
        StructField("operationName", StringType()),
        StructField("operationVersion", StringType()),
        StructField("properties", StructType([
            StructField("actionName", StringType()),
            StructField("logId", StringType()),
            StructField("requestId", StringType()),
            StructField("requestParams", StringType()),
            StructField("response", StringType()),
            StructField("serviceName", StringType()),
            StructField("sessionId", StringType()),
            StructField("sourceIPAddress", StringType()),
            StructField("userAgent", StringType())])),
        StructField("resourceId", StringType()),
        StructField("time", StringType()),
    ])))
]))

bronzeDF = spark.readStream.table(bronze_table)

query = (bronzeDF
         .select("deserializedBody")
         .withColumn("parsedBody", from_json("deserializedBody", raw_schema))
         .select(explode("parsedBody.records").alias("streamRecord"))
         .selectExpr("streamRecord.*")
         .withColumn("version", col("operationVersion"))
         .withColumn("date_time", col("time").cast("timestamp"))
         .withColumn("timestamp", unix_timestamp(col("date_time")) * 1000)
         .withColumn("date", col("time").cast("date"))
         .select("category", "version", "timestamp", "date_time", "date", "properties", col("identity").alias("userIdentity"))
         .selectExpr("*", "properties.*")
         .withColumnRenamed("requestParams", "flattened")
         .withColumn("identity", from_json("userIdentity", "email STRING, subjectName STRING"))
         .withColumn("response", from_json("response", "errorMessage STRING,result STRING,statusCode BIGINT"))
         .drop("properties", "userIdentity")
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
spark.sql(f"OPTIMIZE {silver_table}")

# COMMAND ----------


@udf(StringType())
def just_keys_udf(string):
    return [i for i in json.loads(string).keys()]

# COMMAND ----------


def flatten_table(service):

    service_name = service.replace("-", "_")

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


# For each table name (i.e. event type) create a separate thread and run the ThreadWorker function to save the data to Delta tables.
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
