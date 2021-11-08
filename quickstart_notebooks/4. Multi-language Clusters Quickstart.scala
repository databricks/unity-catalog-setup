// Databricks notebook source
// MAGIC %md
// MAGIC ## Unity Catalog Data Access Controls | Multi-language Support
// MAGIC 
// MAGIC A notebook that provides an example workflow for using multi-language clusters, and how ACL is enforced by UC across all languages
// MAGIC 
// MAGIC **Note**: create a multi-cluster first using the Cluster Setup notebook, and attach this notebook to it
// MAGIC 
// MAGIC **Note 2**: make sure to revoke all permissions from `quickstart_catalog.quickstart_database.quickstart_table`, via the Data Explorer tab under SQL view

// COMMAND ----------

// MAGIC %md
// MAGIC ## Let's look at the quickstart table

// COMMAND ----------

// MAGIC %sql
// MAGIC DESCRIBE extended quickstart_catalog.quickstart_database.quickstart_table

// COMMAND ----------

// MAGIC %sql
// MAGIC SHOW GRANT ON quickstart_catalog.quickstart_database.quickstart_table

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT * FROM quickstart_catalog.quickstart_database.quickstart_table

// COMMAND ----------

// MAGIC %sql
// MAGIC INSERT INTO quickstart_catalog.quickstart_database.quickstart_table VALUES (60, 'SQL')

// COMMAND ----------

// MAGIC %md
// MAGIC ## How about Scala?

// COMMAND ----------

// MAGIC %scala
// MAGIC // scala also enforce permission
// MAGIC val df = spark.table("quickstart_catalog.quickstart_database.quickstart_table")
// MAGIC display(df)

// COMMAND ----------

// MAGIC %scala
// MAGIC val writeDF = Seq(
// MAGIC   (60, "SCALA"),
// MAGIC ).toDF("columnA", "columnB")
// MAGIC writeDF.write.mode("append").saveAsTable("quickstart_catalog.quickstart_database.quickstart_table")

// COMMAND ----------

// MAGIC %md
// MAGIC ## Python

// COMMAND ----------

// MAGIC %python
// MAGIC df = spark.table("quickstart_catalog.quickstart_database.quickstart_table")
// MAGIC display(df)

// COMMAND ----------

// MAGIC %python
// MAGIC 
// MAGIC from pyspark.sql.types import StructType,StructField, StringType, IntegerType
// MAGIC 
// MAGIC schema = StructType([ \
// MAGIC     StructField("columnA", IntegerType(), True), \
// MAGIC     StructField("columnB",StringType(),True)])
// MAGIC df = spark.createDataFrame(data = [(60, "PYTHON")], schema = schema)
// MAGIC df.write.mode("append").saveAsTable("quickstart_catalog.quickstart_database.quickstart_table")

// COMMAND ----------

// MAGIC %md
// MAGIC ### Even R

// COMMAND ----------

// MAGIC %r
// MAGIC library(SparkR)
// MAGIC df <- tableToDF("quickstart_catalog.quickstart_database.quickstart_table")
// MAGIC display(df)

// COMMAND ----------

// MAGIC %r
// MAGIC r_df <- data.frame(
// MAGIC    columnA = c (as.integer(60)), 
// MAGIC    columnB = c("SparkR")
// MAGIC   )
// MAGIC df <- as.DataFrame(r_df)
// MAGIC saveAsTable(df, "quickstart_catalog.quickstart_database.quickstart_table", mode="append")

// COMMAND ----------

// MAGIC %r
// MAGIC library(sparklyr)
// MAGIC library(dplyr)
// MAGIC # create a sparklyr connection
// MAGIC sc <- spark_connect(method = "databricks")
// MAGIC df <- sdf_sql(sc, "SELECT * FROM quickstart_catalog.quickstart_database.quickstart_table")
// MAGIC head(df)

// COMMAND ----------

// MAGIC %r
// MAGIC r_df <- data.frame(
// MAGIC    columnA = c (as.integer(60)), 
// MAGIC    columnB = c("sparklyr")
// MAGIC   )
// MAGIC 
// MAGIC df <- copy_to(sc, r_df, overwrite=TRUE)
// MAGIC spark_write_table(df, "quickstart_catalog.quickstart_database.quickstart_table", mode="append")

// COMMAND ----------

// MAGIC %md
// MAGIC ## Now let's grant SELECT permission

// COMMAND ----------

// MAGIC %sql
// MAGIC --- need USAGE permission on DATABASE level as well
// MAGIC GRANT USAGE on DATABASE quickstart_catalog.quickstart_database to `account users`;
// MAGIC GRANT SELECT on quickstart_catalog.quickstart_database.quickstart_table to `account users`;

// COMMAND ----------

// MAGIC %md
// MAGIC ## Query across all languages would now work, but write would fail
// MAGIC Go back and re-run Cmd 2-15

// COMMAND ----------

// MAGIC %md
// MAGIC ## Now grant MODIFY permission as well

// COMMAND ----------

// MAGIC %sql
// MAGIC GRANT MODIFY on quickstart_catalog.quickstart_database.quickstart_table to `account users`

// COMMAND ----------

// MAGIC %md
// MAGIC ## Query and write across all languages work like magic
// MAGIC Go back and re-run Cmd 2-15

// COMMAND ----------

// MAGIC %md
// MAGIC ## Clean up permissions

// COMMAND ----------

// MAGIC %sql
// MAGIC REVOKE ALL PRIVILEGES on quickstart_catalog.quickstart_database.quickstart_table FROM `account users`;

// COMMAND ----------


