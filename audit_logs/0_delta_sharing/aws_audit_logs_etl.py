# Databricks notebook source
# MAGIC %md
# MAGIC #Prerequisites
# MAGIC In order to make this real audit log data using our <a href="https://docs.databricks.com/administration-guide/account-settings/audit-logs.html">Configure Databricks Audit Logging Feature</a> and you must have the premium plan  <a href="https://databricks.com/product/aws-pricing">Pricing Details</a>. Just email your <b>Account Representative</b> to upgrade your workspace, no migration necessary to get this tier.
# MAGIC - <a href="https://docs.google.com/document/d/1lO66KIrTF5C8Zzt-Pvzw23Y1zyEWU0Z2OGoLvFBEFkE/edit#heading=h.1dtudqy417mb">Need to run on a single-user isolated (PE) cluster</a>, Databricks Runtime 10.2+
# MAGIC - Require an instance profile for access to the log bucket (where audit logs are delivered), and the sink bucket (location for AutoLoader schema and streaming checkpoints)

# COMMAND ----------

# DBTITLE 1,Use Default UC Catalog
spark.sql(f"USE CATALOG main")

# COMMAND ----------

# DBTITLE 1,Create Widgets to Input Necessary Parameters (run once then comment out when scheduling automated job)
# MAGIC %sql
# MAGIC CREATE WIDGET TEXT database DEFAULT "audit_logs";
# MAGIC CREATE WIDGET TEXT log_bucket DEFAULT "s3://";
# MAGIC CREATE WIDGET TEXT sink_bucket DEFAULT "s3://"

# COMMAND ----------

# DBTITLE 1,Get Defined Parameters from Widgets to Populate Throughout this Notebook
database = dbutils.widgets.get("database")
log_bucket = dbutils.widgets.get("log_bucket")
sink_bucket = dbutils.widgets.get("sink_bucket").strip("/")

# COMMAND ----------

# DBTITLE 1,Register Database in Metastore
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {database}")

# COMMAND ----------

# DBTITLE 1,CLEAN UP AND USE ONLY IF PREVIOUSLY RAN WITHOUT A UC ENABLED CLUSTER
#silver_path = f"{sink_bucket}/audit_logs_streaming/silver"
#bronze_path = f"{sink_bucket}/audit_logs_streaming/bronze"
#gold_path = f"{sink_bucket}/audit_logs_streaming/gold"
#checkpoint_path = f"{sink_bucket}/checkpoints"
#dbutils.fs.rm(bronze_path, True)
#dbutils.fs.rm(checkpoint_path, True)
#dbutils.fs.rm(silver_path, True)
#dbutils.fs.rm(gold_path, True)

# COMMAND ----------

# DBTITLE 1,Step 0 - Input Sink & Source Bucket Parameters
# MAGIC %md
# MAGIC <b>Customer to Reuse</b> : process your own Databricks audit logs by inputting the prefix where Databricks delivers them (select `s3bucket` in the **Data Source** widget and input the proper prefix to **Audit Logs Source Bucket** `(example: s3a://demo2021-log-delivery/auditlogs-data/)` and the proper **Delta Lake Sink Bucket** `(example: s3://lakehouse2021-demo/) widgets above)`
# MAGIC <br> *** Treat this as a framework with which to consider incremental data processing with Structured Streaming and how to handle nested data**

# COMMAND ----------

# MAGIC %md
# MAGIC ### Audit Log Delivery
# MAGIC Databricks provides access to audit logs of activities performed by Databricks users, allowing your enterprise to monitor detailed Databricks usage patterns.
# MAGIC - <b>Latency</b>: After initial setup or other configuration changes, expect some delay before your changes take effect. For initial setup of audit log delivery, it takes up to one hour for log delivery to begin. After log delivery begins, auditable events are typically logged within 15 minutes. Additional configuration changes typically take an hour to take effect.
# MAGIC - <b>Encryption</b>: Databricks encrypts audit logs using Amazon S3 server-side encryption.
# MAGIC - <b>Format</b>: Databricks delivers audit logs in JSON format.
# MAGIC - <b>Location:</b> The delivery location is <b> bucket-name/delivery-path-prefix/workspaceId=workspaceId/date=yyyy-mm-dd/auditlogs_internal-id.json </b>. New JSON files are delivered every few minutes, potentially - overwriting existing files for each workspace. The delivery path is defined as part of the configuration.
# MAGIC Databricks can overwrite the delivered log files in your bucket at any time. If a file is overwritten, the existing content remains, but there may be additional lines for more auditable events.
# MAGIC Overwriting ensures exactly-once semantics without requiring read or delete access to your account.
# MAGIC 
# MAGIC #### Dictionary Documentation & Data Model Build Out
# MAGIC <a href="https://docs.databricks.com/data-governance/delta-sharing/#audit-access-and-activity-for-delta-sharing-resources">Audit Log Documentation Details</a>

# COMMAND ----------

# DBTITLE 1,Why use Structured Streaming and Delta Lake?
# MAGIC %md
# MAGIC #### Streaming
# MAGIC * Incremental processing
# MAGIC * Latency 
# MAGIC * Automatic bookkeeping
# MAGIC 
# MAGIC #### Delta
# MAGIC * Reliability
# MAGIC * Performance

# COMMAND ----------

# MAGIC %md
# MAGIC Start off by importing the required libraries, which should be included in the most recent Databricks runtimes. You can think of runtimes as conveniently prepackaged environments that allow you to do 90% of your work out of the box, but it's also easy to add any libaries you might need. You can check specific library versions in the [release notes](https://docs.databricks.com/release-notes/runtime/8.1.html#system-environment).

# COMMAND ----------

# DBTITLE 1,Import Libraries
from pyspark.sql.functions import udf, col, from_unixtime, from_utc_timestamp, from_json
from pyspark.sql.types import StringType, StructField, StructType
import json, time, requests

# COMMAND ----------

# MAGIC %md
# MAGIC Databricks delivers audit logs daily to a customer-specified S3 bucket in the form of JSON. Rather than writing logic to determine the state of our [Delta Lake](https://docs.databricks.com/delta/index.html) tables, we're going to utilize [Structured Streaming](https://docs.databricks.com/spark/latest/structured-streaming/index.html)'s write-ahead logs and checkpoints to maintain the state of our tables. In this case, we've designed our ETL to run once per day, so we're using a `file source` with `triggerOnce` to simulate a batch workload with a streaming framework. Since Structured Streaming requires that we explicitly define the schema, we'll read the raw JSON files to get it.

# COMMAND ----------

# MAGIC %md
# MAGIC We instantiate our `StreamReader` using the schema we inferred and the path to the raw audit logs.

# COMMAND ----------

# DBTITLE 1,Best Practices with using a file-based source
# MAGIC %md
# MAGIC For larger scale deployments, we recommend the use of the newly-developed [Databricks Auto Loader datasource](https://docs.databricks.com/spark/latest/structured-streaming/auto-loader.html). Spark file-based datasources with blob storage rely on listing all the files during each microbatch and processing all new files since the last execution, which can be very slow once you have a large number of files. Auto Loader changes this paradigm by utilize SNS / SQS to subscribe to changes in the input directory, so that you don't have to list the files.

# COMMAND ----------

# DBTITLE 1,Read in JSON Logs via Auto-Loader
streamDF = (
  spark
  .readStream
  .format("cloudFiles")
  .option("cloudFiles.format", "json")
  .option("cloudFiles.inferColumnTypes", True)
  .option("cloudFiles.schemaHints", "workspaceId long")  
  .option("cloudFiles.schemaLocation", f"{sink_bucket}/audit_log_schema")
  .load(f"{log_bucket}")
)

# COMMAND ----------

# MAGIC %md
# MAGIC We then instantiate our `StreamWriter` and write out the raw audit logs into a bronze Delta Lake table that's partitioned by date.

# COMMAND ----------

# DBTITLE 1,Let's talk about some of the options we've specified below
# MAGIC %md
# MAGIC * `format("delta")` - this is all you need to do to write out your data to Delta Lake. Why is this important?
# MAGIC   * It gives us ACID transactions, so we're able to continuously stream to our tables while allowing analysts to query it with no downtime - just like a database. If our pipeline fails for some reason, it's all or nothing for the writes and we don't have to worry about partial data. If we have any changes / updates after the fact, we can easily do that using a one-time batch operation.
# MAGIC * `outputMode("append")` - in this pipeline, we don't expect any modifications to existing records, so all our job does is add new records. Regardless, we recommend that you append to your bronze table so that you can easily replay your entire data pipeline. Even though we're using a file source / Auto Loader here, if you're using something like a messaging bus, it's much easier and cheaper to replay from Delta Lake
# MAGIC * `option("checkpointLocation", "{}/checkpoints/bronze".format(sinkBucket))` - we won't get too deep into checkpoints, but this is the mechanism by which Structured Streaming automatically keeps track of the data already processed since the previous execution. This option specifies the prefix to which it's writing.
# MAGIC *This is handled by Live Delta Tables!*
# MAGIC * `option("mergeSchema", True)` - Delta Lake, by default, enforces schema, which helps you bring relability to your data lake. However, for this use case, we don't have control over the upstream schema and we know that teams are constantly adding new services to our audit logs, which will change our "fat" struct `requestParams`. The takeaway here is that you can set this option based on your use case - if you want the pipeline to fail because of an unexpected schema change, ignore this option and the write will fail. It's up to you how permissive you want the pipeline to be (and in which way)
# MAGIC * `trigger(once=True)` - this bears diving into a bit more detail, so see the consideratiosn below

# COMMAND ----------

# DBTITLE 1,Considerations on Triggers
# MAGIC %md
# MAGIC Structured Streaming supports different trigger types, which control how frequently it executes a new microbatch. Trigger selection depends on several factors, with frequency of data generation from your source and your SLA (service level agreement) for downstream data availability being two of the most important.
# MAGIC 
# MAGIC In this case, we utilize `TriggerOnce`, which you can think of as operating in pseudo-batch mode - it only runs once every time we execute a [Databricks Job](https://docs.databricks.com/jobs.html). If you're not familiar with jobs, they're scheduled runs of some set of code / logic (e.g., hourly or daily). There are a few reasons we run it with this trigger:
# MAGIC * it's more cost efficient
# MAGIC * data only appears every 15 minutes and we don't need the data to be available right away
# MAGIC * we schedule the Databricks job to run every hour and have the job process all new files since the last time it was executed - in this way, there's only one microbatch per execution, hence referring to it as pseudo-batch

# COMMAND ----------

# DBTITLE 1,Write JSON Data to Managed Delta Bronze Table
bronze_path = f"{sink_bucket}/audit_logs_streaming/bronze"
checkpoint_path = f"{sink_bucket}/checkpoints"

(streamDF
 .writeStream
 .format("delta")
 .partitionBy("date")
 .outputMode("append")
 .option("checkpointLocation", f"{checkpoint_path}/bronze")
 .option("mergeSchema", True) 
 .trigger(once=True)
 .toTable(f"{database}.bronze")
)

# COMMAND ----------

# MAGIC %md
# MAGIC Since the stream runs in a separate thread from the other processes, we want to ensure that the query finishes before we proceed. We can use information available via the SparkSession (specifically `spark.streams.active`) to check before proceeding. **Note:** you only need this if you're combining enrichment steps in the same notebook (i.e., bronze --> silver)

# COMMAND ----------

# DBTITLE 1,Wait for Data to Write out to Bronze Delta Table
while spark.streams.active != []:
  print("Waiting for streaming query to finish.")
  time.sleep(5)

# COMMAND ----------

# MAGIC %md
# MAGIC Now that we've created the table on an external S3 bucket, we'll need to register the table to the internal Databricks Hive metastore to make access to the data easier for end users <b>(SQLA)</b>. We'll create both the database/schema `uc_audit_logs`, in addition to the `bronze` table. All of Delta Lake's metadata are stored in the transaction log, so the entry in the Hive metastore is just a pointer. In addition to providing ACID transactions and schema evolution, partitioning is much more scalable than Hive and there's functionally no limit to the number of partitions you can have in a Delta Lake table

# COMMAND ----------

# DBTITLE 1,Auto-Optimize
# MAGIC %md
# MAGIC If you update your Delta Lake tables in batch or pseudo-batch fashion, it's best practice to run `OPTIMIZE` immediately following an update. Previously, for our use case, we only received audit logs once per day so the job only ran once per day and we could run optimize right after each run. However, our use case changed in that we're receiving logs more frequently, so we might consider running optimize just once per day instead of after every batch

# COMMAND ----------

# DBTITLE 1,Optimize and Compact the Files for Faster I/O Reads on Databricks SQL
spark.sql(f"OPTIMIZE {database}.bronze")

# COMMAND ----------

# MAGIC %md
# MAGIC Since we ship audit logs for all services in a single JSON, we've defined a struct called `requestParams` which contains a union of the keys for all services. Eventually, we're going to create individual tables for each service, so we want to strip down the `requestParams` field for each table so that it contains only the relevant keys for the service. To accomplish this, we define UDF to strip away all keys in `requestParams` that have `null` values.

# COMMAND ----------

# DBTITLE 1,Remove Nulls from Request Params for Silver Delta Directory (Exploration/Slightly Cleansed Data Layer)
def stripNulls(raw):
  return json.dumps({i: raw.asDict()[i] for i in raw.asDict() if raw.asDict()[i] != None})
strip_udf = udf(stripNulls, StringType())

# COMMAND ----------

strip_udf = udf(stripNulls, StringType())

# COMMAND ----------

# DBTITLE 1,Step 8: We instantiate a StreamReader from our bronze Delta Lake table to stream updates to our Silver Delta Lake table
bronzeDF = spark.readStream.format("delta").table(f"{database}.bronze")

# COMMAND ----------

# DBTITLE 1,Silver Transformations Explained
# MAGIC %md
# MAGIC We apply the following transformations when going from the bronze Delta Lake table to the silver Delta Lake table:
# MAGIC * strip the `null` keys from `requestParams` and store the output as a string
# MAGIC * parse `email` from `userIdentity`
# MAGIC * parse an actual timestamp from the `timestamp` field and store it in `date_time`
# MAGIC * drop the raw `requestParams` and `userIdentity`

# COMMAND ----------

# DBTITLE 1,Apply Silver Transformations from Bronze Delta Table and Write out to Silver Delta Table (Cleansed for Exploration)
query = (
  bronzeDF
  .withColumn("flattened", strip_udf("requestParams"))
  .withColumn("email", col("userIdentity.email"))
  .withColumn("date_time", from_utc_timestamp(from_unixtime(col("timestamp")/1000), "UTC"))
  .drop("requestParams")
  .drop("userIdentity")
)

# COMMAND ----------

# MAGIC %md
# MAGIC We then stream our changes from the bronze Delta Lake table to the silver Delta Lake table. The nice thing about Delta Lake is that it's also a source for Structured Streaming, so it's simple to build these enrichment pipelines.

# COMMAND ----------

# DBTITLE 1,Write out Silver Pipeline to Managed Silver Delta Table
silver_path = f"{sink_bucket}/audit_logs_streaming/silver"

(query
 .writeStream
 .format("delta")
 .partitionBy("date")
 .outputMode("append")
 .option("checkpointLocation", f"{checkpoint_path}/silver")
 .option("mergeSchema", True)
 .trigger(once=True)
 .toTable(f"{database}.silver")
)

# COMMAND ----------

# MAGIC %md
# MAGIC Since the stream runs in a separate thread from the other processes, we want to ensure that the query finishes before we proceed. We can use information available via the SparkSession (specifically `spark.streams.active`) to check before proceeding.

# COMMAND ----------

while spark.streams.active != []:
  print("Waiting for streaming query to finish.")
  time.sleep(5)

# COMMAND ----------

# MAGIC %md
# MAGIC As mentioned before, we now run `OPTIMIZE` on the silver Delta Lake table.

# COMMAND ----------

# DBTITLE 1,As mentioned before, we now run OPTIMIZE on the silver Delta Lake table.
spark.sql(f"OPTIMIZE {database}.silver")

# COMMAND ----------

# MAGIC %md
# MAGIC In the final step of our ETL process, we first define a UDF to parse the keys from the stripped down version of the original `requestParams` field. 

# COMMAND ----------

def justKeys(string):
  return [i for i in json.loads(string).keys()]
just_keys_udf = udf(justKeys, StringType())

# COMMAND ----------

# DBTITLE 1,Gold - Consumption Layer
# MAGIC %md
# MAGIC One of the objectives of this workshop is to not "over-ETL" a JSON-based pipeline, so let's keep that in mind as we discuss the strategy for our gold tables. Also bear in mind that, even though we talk about medallion architecture with bronze, silver, and gold, you don't need to restrict yourself just 3 layers. Think of it more generally as a raw layer (bronze), any number of intermediate layers (silver), and a serving layer (gold).
# MAGIC 
# MAGIC In our case, the audit logs themselves are split into different services on Databricks (like clusters and mlflow). In order to balance accessibility with simplicity, we decided that our gold layer should have a separate table for each of these services. Another consideration is that the `requestParams` field, which we referred to earlier as a "fat" struct, contains all possible parameters across all services to ensure the schema is consistent within each new raw JSON file. Closer inspection reveals that each service only contains a subset of the full list, so rather than continue to utilize that struct, we create a customized schema per service using our UDF. You could flatten the struct for each service, but even within a given service, it could be pretty sparsely populated. With all that in mind, we decided to create a custom struct per service, not flatten it, and allow end users to utilize built-in functionality in Spark SQL to access the fields in which they're interested

# COMMAND ----------

# DBTITLE 1,Gold ETL Explained
# MAGIC %md
# MAGIC Define a function which accomplishes the following:
# MAGIC * gathers the keys for each record for a given `serviceName`
# MAGIC * creates a set of those keys (to remove duplicates)
# MAGIC * creates a schema from those keys to apply to a given `serviceName` (if the `serviceName` does not have any keys in `requestParms`, we give it one key schema called `placeholder`)
# MAGIC * write out to individual gold Delta Lake tables for each `serviceName` in the silver Delta Lake table

# COMMAND ----------

# DBTITLE 1,Gold Layer Transformation(s)
def flatten_table(service_name, gold_path):
  flattenedStream = spark.readStream.format("delta").table(f"{database}.silver")
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
   .option("mergeSchema", True)
   .trigger(once=True)
   .toTable(f"{database}.{service_name}")
  )

# COMMAND ----------

# DBTITLE 1,This cell gets a list of the distinct values in serviceName and collects them as a Python list so we can iterate.
service_name_list = [i['serviceName'] for i in spark.table(f"{database}.silver").select("serviceName").distinct().collect()]

# COMMAND ----------

# DBTITLE 1,We then run the flattenTable function for each `serviceName`.
gold_path = f"{database}"

for service_name in service_name_list:
  flatten_table(service_name, gold_path)

# COMMAND ----------

# MAGIC %md
# MAGIC Since the stream runs in a separate thread from the other processes, we want to ensure that the query finishes before we proceed. We can use information available via the SparkSession (specifically `spark.streams.active`) to check before proceeding.

# COMMAND ----------

while spark.streams.active != []:
  print("Waiting for streaming query to finish.")
  time.sleep(5)

# COMMAND ----------

# MAGIC %md
# MAGIC Now that we've created the table on an external S3 bucket, we'll need to register each of the gold tables to the internal Databricks Hive metastore to make access to the data easier for end users. 

# COMMAND ----------

# DBTITLE 1,Optimize Each Gold Service table
for service_name in service_name_list:
  spark.sql(f"OPTIMIZE {database}.{service_name}")

# COMMAND ----------

# DBTITLE 1,Generate A Data Dimension Table
# MAGIC %scala
# MAGIC 
# MAGIC import java.time.{Instant,LocalDate, LocalDateTime, ZoneOffset};
# MAGIC import org.apache.spark.sql.DataFrame;
# MAGIC import org.apache.spark.sql.types.IntegerType;
# MAGIC import org.apache.spark.sql.functions.{date_add, lit};
# MAGIC 
# MAGIC /*
# MAGIC   Generate a date dimensions table to index dates
# MAGIC */
# MAGIC 
# MAGIC // Get the origin epoch date
# MAGIC val originDate : LocalDate = LocalDateTime.ofInstant(Instant.EPOCH, ZoneOffset.UTC)
# MAGIC                                           .toLocalDate(); 
# MAGIC 
# MAGIC // Get a destination date 100 years from epoch
# MAGIC val daysToGenerate : Int = 365 * 100;
# MAGIC 
# MAGIC // Create a dataframe for date dimensions resolution
# MAGIC val df_time : DataFrame = spark.range(daysToGenerate)
# MAGIC                                .toDF("DATEID")
# MAGIC                                .withColumn("DATEID", $"DATEID".cast(IntegerType))
# MAGIC                                .withColumn("DATE", date_add(lit(originDate), $"DATEID"))
# MAGIC 
# MAGIC df_time.createOrReplaceTempView("time_id");

# COMMAND ----------

# DBTITLE 1,Generate A Data Dimension Table
# MAGIC %sql
# MAGIC 
# MAGIC --
# MAGIC -- CREATE DATE DIMENSION TABLE
# MAGIC --
# MAGIC USE CATALOG MAIN; 
# MAGIC CREATE TABLE IF NOT EXISTS audit_logs.DIM_DATE
# MAGIC AS
# MAGIC   SELECT DATEID,
# MAGIC          DATE,
# MAGIC          DATE_FORMAT(DATE, 'yyyyMMdd') AS DATE_KEY,
# MAGIC          QUARTER(DATE) AS QUARTER,
# MAGIC          
# MAGIC          DAY(DATE) AS DAY,
# MAGIC          MONTH(DATE) AS MONTH_NUM,
# MAGIC          YEAR(DATE) AS YEAR,
# MAGIC          WEEKOFYEAR(DATE) AS WEEK_NUM,
# MAGIC          DAYOFWEEK(DATE) AS WEEK_DAY,
# MAGIC          DAYOFYEAR(DATE) AS YEAR_DAY,
# MAGIC          DATE_FORMAT(DATE, 'E') AS DAY_OF_WEEK_SHORT,
# MAGIC          DATE_FORMAT(DATE, 'EEEE') AS DAY_OF_WEEK_LONG,
# MAGIC          DATE_FORMAT(DATE, 'MMMM') AS MONTH
# MAGIC   FROM TIME_ID;
# MAGIC   
# MAGIC -- view the temporary table
# MAGIC SELECT *
# MAGIC FROM DIM_DATE
# MAGIC WHERE DATE <= CURRENT_TIMESTAMP
# MAGIC ORDER BY DATE ASC;

# COMMAND ----------

# DBTITLE 1,Optimize Date Dimension Table
# MAGIC %sql
# MAGIC OPTIMIZE audit_logs.DIM_DATE

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE audit_logs.DIM_DATE

# COMMAND ----------

# DBTITLE 1,We now have a gold Delta Lake table for each serviceName that Databricks tracks in its audit logs, which we can now use for monitoring and analysis.
display(spark.sql(f"SHOW TABLES IN {database}"))

# COMMAND ----------


