// Databricks notebook source
import org.apache.spark.eventhubs.{ConnectionStringBuilder, EventHubsConf, EventPosition}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.streaming.Trigger

import java.util.UUID

// COMMAND ----------

val sinkBucket = "s3://databricks-uc-field-eng-kpwfklmr/az_audit"
val database = "az_audit_logs"
val isFirstRun = !spark.catalog.tableExists(s"${database}.bronze")

// COMMAND ----------

def structFromJson(df: DataFrame, c: String): Column = {
  require(df.schema.fields.map(_.name).contains(c), s"The dataframe does not contain col $c")
  require(df.schema.fields.filter(_.name == c).head.dataType.isInstanceOf[StringType], "Column must be a json formatted string")
  val jsonSchema = spark.read.json(df.select(col(c)).filter(col(c).isNotNull).as[String]).schema
  if (jsonSchema.fields.map(_.name).contains("_corrupt_record")) {
    println(s"WARNING: The json schema for column $c was not parsed correctly, please review.")
  }
  from_json(col(c), jsonSchema).alias(c)
}

// COMMAND ----------

val runID = UUID.randomUUID().toString.replace("-", "")
val connectionString = dbutils.secrets.get("azure", "ehConnectionString")
val ehConf = if (isFirstRun) EventHubsConf(connectionString).setStartingPosition(EventPosition.fromStartOfStream) else EventHubsConf(connectionString)

val streamDF = spark
  .readStream
  .format("eventhubs")
  .options(ehConf.toMap)
  .load()
  .withColumn("payload", 'body.cast("string"))
  .withColumn("runID", lit(runID))

// COMMAND ----------

val rawPath = s"${sinkBucket}/audit_logs_streaming/raw"
val checkpointPath = s"${sinkBucket}/checkpoints"

streamDF
  .writeStream
  .format("delta")
  .partitionBy("runID")
  .outputMode("append")
  .option("checkpointLocation", s"${checkpointPath}/raw")
  .option("path", rawPath) 
  .option("mergeSchema", true)
  .trigger(Trigger.Once())
  .start()

// COMMAND ----------

while (!spark.streams.active.isEmpty) {
  println("Waiting for streaming query to finish.")
  Thread.sleep(5000)
}

// COMMAND ----------

val rawBodyLookup = spark.read.format("delta").load(rawPath).filter('runID === lit(runID))
if (rawBodyLookup.rdd.isEmpty()) {
   dbutils.notebook.exit("No records to process")
}
val schemaBuilders = rawBodyLookup
  .withColumn("parsedBody", structFromJson(rawBodyLookup, "payload"))
  .select(explode($"parsedBody.records").alias("streamRecord"))
  .selectExpr("streamRecord.*")
  .withColumn("version", 'operationVersion)
  .withColumn("time", 'time.cast("timestamp"))
  .withColumn("timestamp", unix_timestamp('time) * 1000)
  .withColumn("date", 'time.cast("date"))
  .select('category, 'version, 'timestamp, 'date, 'properties, 'identity.alias("userIdentity"))
  .selectExpr("*", "properties.*").drop("properties")

// COMMAND ----------

val parsedDF = rawBodyLookup
  .withColumn("parsedBody", structFromJson(rawBodyLookup, "payload"))
  .select(explode($"parsedBody.records").alias("streamRecord"))
  .selectExpr("streamRecord.*")
  .withColumn("version", 'operationVersion)
  .withColumn("time", 'time.cast("timestamp"))
  .withColumn("timestamp", unix_timestamp('time) * 1000)
  .withColumn("date", 'time.cast("date"))
  .select('category, 'version, 'timestamp, 'date, 'properties, 'identity.alias("userIdentity"))
  .withColumn("userIdentity", structFromJson(schemaBuilders, "userIdentity"))
  .selectExpr("*", "properties.*").drop("properties")
  .withColumn("requestParams", structFromJson(schemaBuilders, "requestParams"))
  .withColumn("response", structFromJson(schemaBuilders, "response"))
  .drop("logId")

// COMMAND ----------

val bronzePath = s"${sinkBucket}/audit_logs_streaming/bronze"

parsedDF
  .write
  .format("delta")
  .partitionBy("date")
  .mode("append")
  .option("mergeSchema", true)
  .save(bronzePath) 

// COMMAND ----------

spark.sql(s"CREATE DATABASE IF NOT EXISTS ${database}")

// COMMAND ----------

spark.sql(s"""
CREATE TABLE IF NOT EXISTS ${database}.bronze
USING DELTA
LOCATION '${bronzePath}'
""")

// COMMAND ----------

spark.sql(s"OPTIMIZE ${database}.bronze")

// COMMAND ----------

val bronzeDF = spark.readStream.load(bronzePath)

val query = (
  bronzeDF
  .withColumn("email", col("userIdentity.email"))
  .withColumn("date_time", from_utc_timestamp(from_unixtime(col("timestamp")/1000), "UTC"))
  .drop("userIdentity")
)

// COMMAND ----------

val silverPath = s"${sinkBucket}/audit_logs_streaming/silver"

query
  .writeStream
  .format("delta")
  .partitionBy("date")
  .outputMode("append")
  .option("checkpointLocation", s"${checkpointPath}/silver")
  .option("path", silverPath)
  .option("mergeSchema", true)
  .trigger(Trigger.Once())
  .start()

// COMMAND ----------

while (!spark.streams.active.isEmpty) {
  println("Waiting for streaming query to finish.")
  Thread.sleep(5000)
}

// COMMAND ----------

spark.sql(s"""
CREATE TABLE IF NOT EXISTS ${database}.silver
USING DELTA
LOCATION '${silverPath}'
""")

// COMMAND ----------

assert(spark.table(s"${database}.bronze").count() == spark.table(f"${database}.silver").count())

// COMMAND ----------

spark.sql(s"OPTIMIZE ${database}.silver")

// COMMAND ----------

def flatten_table(serviceName: String, goldPath: String): Unit = {
  val flattenedStream = spark.readStream.format("delta").table(s"${database}.silver")
  val flattened = spark.table(s"${database}.silver")
    
  flattenedStream
    .filter('serviceName === lit(serviceName))
    .writeStream
    .partitionBy("date")
    .outputMode("append")
    .format("delta")
    .option("checkpointLocation", s"${checkpointPath}/gold/${serviceName}")
    .option("path", s"${goldPath}/${serviceName}")
    .option("mergeSchema", true)
    .trigger(Trigger.Once())
    .start()
}

// COMMAND ----------

val serviceNames = spark.table(s"${database}.silver").select("serviceName").distinct().collect().map(c => c.getString(0))
val goldPath = s"${sinkBucket}/audit_logs_streaming/gold"

for (serviceName <- serviceNames) {
  flatten_table(serviceName, goldPath)
}

// COMMAND ----------

while (!spark.streams.active.isEmpty) {
  println("Waiting for streaming query to finish.")
  Thread.sleep(5000)
}

// COMMAND ----------

for (serviceName <- serviceNames) {
  spark.sql(s"""
  CREATE TABLE IF NOT EXISTS ${database}.${serviceName}
  USING DELTA
  LOCATION '${goldPath}/${serviceName}'
  """)
  spark.sql(s"OPTIMIZE ${database}.${serviceName}")
}

// COMMAND ----------

display(spark.sql(s"SHOW TABLES IN ${database}"))
