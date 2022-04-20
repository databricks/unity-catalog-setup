# Databricks notebook source
import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *
import json, time

# retrieve Shared Access Key from secret scope
connSharedAccessKey = dbutils.secrets.get(spark.conf.get("mypipeline.secret_scope_name"), spark.conf.get("mypipeline.secret_name"))

# Name of Eventhubs namespace
eh_ns_name = spark.conf.get("mypipeline.eh_ns_name")
eh_topic_name = spark.conf.get("mypipeline.eh_topic_name")

# event hub configuration
BOOTSTRAP_SERVERS = f"{eh_ns_name}.servicebus.windows.net:9093"
EH_SASL = f'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="$ConnectionString" password="{connSharedAccessKey}";'

@dlt.table(
  comment="The raw audit logs, ingested from the event hub configured with Databricks audit log configuration",
  table_properties={"quality":"bronze"},
  partition_cols = [ 'date' ]
)
def bronze():
    return (spark.readStream
            .format("kafka")
            .option("subscribe", eh_topic_name)
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

@udf(StringType())
def strip_null_udf(raw):
    return json.dumps({i: raw.asDict()[i] for i in raw.asDict() if raw.asDict()[i] != None})

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
  
@dlt.table(
    comment="Audit logs cleaned and prepared for analysis. Strip out all empty keys for every record, parse email address from a nested field and parse UNIX epoch to UTC timestamp.",
    table_properties={"quality":"silver"},
    partition_cols = [ 'date' ]
)
def silver():
    return (
        dlt.read_stream("bronze")
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

# these udfs is used to calculate the super-schema based on all events of a given service

@udf(StringType())
def just_keys_udf(string):
    return [i for i in json.loads(string).keys()]

@udf(StringType())
def extract_schema_udf(keys):

    schema = StructType()

    keysList = [i[1:-1].split(", ") for i in keys]

    keysDistinct = {key for keys in keysList for key in keys if key != ""}

    if len(keysDistinct) == 0:
        schema.add(StructField("placeholder", StringType()))
    else:
        for key in keysDistinct:
            schema.add(StructField(key, StringType()))

    return schema.json()

@dlt.table(
    comment="List of services and their corresponding super-schema"
)
def silver_services_schema():
    
    return (dlt.read("silver")
            .select('serviceName', just_keys_udf(col("flattened")).alias("keys"))
            .groupBy('serviceName').agg(extract_schema_udf(collect_set("keys")).alias("schema"))
           )
