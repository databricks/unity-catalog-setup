# Databricks notebook source
import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *
import json, time

log_bucket = "s3://databricks-e2-certification-logs-bwp2xp/audit-logs"
sink_bucket = "dbfs:/tmp"

@dlt.table(
  comment="The raw audit logs, ingested from the s3 location configured with Databricks audit log configuration",
  table_properties={"quality":"bronze"},
  partition_cols = [ 'date' ]
)
def bronze():
    return (spark
            .readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "json")
            .option("cloudFiles.inferColumnTypes", True)
            .option("cloudFiles.schemaHints", "workspaceId long")  
            .option("cloudFiles.schemaLocation", f"{sink_bucket}/audit_log_schema")
            .load(log_bucket)
           )

# COMMAND ----------

# create silver table
@udf(StringType())
def strip_null_udf(raw):
    return json.dumps({i: raw.asDict()[i] for i in raw.asDict() if raw.asDict()[i] != None})

@dlt.table(
  comment="Audit logs cleaned and prepared for analysis.",
  table_properties={"quality":"silver"},
  partition_cols = [ 'date' ]    
)
def silver():
    return (
        dlt.read_stream("bronze")
          .withColumn("flattened", strip_null_udf("requestParams"))
          .withColumn("email", col("userIdentity.email"))
          .withColumn("date_time", from_utc_timestamp(from_unixtime(col("timestamp")/1000), "UTC"))
          .drop("requestParams")
          .drop("userIdentity")
      )

# COMMAND ----------

@dlt.table(
    comment="Verify bronze & silver tables match"
)
@dlt.expect_or_fail("no_rows_dropped", "silver_count == bronze_count")
def bronze_silver_verification():
    return spark.sql("""SELECT * FROM
  (SELECT COUNT(*) AS bronze_count FROM LIVE.bronze),
  (SELECT COUNT(*) AS silver_count FROM LIVE.silver)""")

# COMMAND ----------

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
    comment="Services and their schemas"
)
def silver_services_schema():
    
    return (dlt.read("silver")
            .select('serviceName', just_keys_udf(col("flattened")).alias("keys"))
            .groupBy('serviceName').agg(extract_schema_udf(collect_set("keys")).alias("schema"))
           )
