# Databricks notebook source
# MAGIC %md
# MAGIC ## Collect column-level lineage and populate a Delta table
# MAGIC 
# MAGIC **Note**: UC Lineage is currently in Private Preview
# MAGIC 
# MAGIC This notebook exports column-level lineage from all UC tables under a schema (can be extended to collect lineage from all UC tables under a catalog)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Provide parameters in the widgets
# MAGIC  
# MAGIC Need to provide 2 parameters
# MAGIC - Provide the 2L name of the schema (`catalog.schema`)
# MAGIC - Provide the target delta table where the column-level lineage will be exported (this table will be created if not exists)

# COMMAND ----------

dbutils.widgets.removeAll()

# COMMAND ----------

dbutils.widgets.text("2L schema name", "arana_lineage.arana_lineage_demo")
dbutils.widgets.text("Target lineage table (3L)", "vuongnguyen.default.column_lineage")

# COMMAND ----------

schema_2L_name = dbutils.widgets.get("2L schema name")
lineage_table = dbutils.widgets.get("Target lineage table (3L)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prepare import

# COMMAND ----------

import requests
import json
from pyspark.sql.functions import explode, lit, concat_ws
from delta.tables import *
from functools import reduce

# COMMAND ----------

host = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().getOrElse(None)
token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().getOrElse(None)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prepare target table to store lineage

# COMMAND ----------

## create the delta table to store the column-level lineage
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {lineage_table} (
      table_name STRING,
      column_name STRING,
      flow STRING,
      lineage_table_name STRING,
      lineage_column_name STRING,
      table_type STRING,
      workspace_id LONG
    ) USING DELTA COMMENT 'This is my column lineage information'
      """)

# COMMAND ----------

# extract lineage information from the json output
def format_lineage(flow, column_lineage):
    return (column_lineage
            .select(explode('columns').alias('col'))
            .select('col.*')
            .withColumnRenamed('name', 'lineage_column_name')
            .withColumn('lineage_table_name', concat_ws('.', 'catalog_name', 'schema_name', 'table_name'))
            .drop('catalog_name', 'schema_name', 'table_name')          
            .withColumn('table_name', lit(table_name))
            .withColumn('column_name', lit(column['name']))
            .withColumn('flow', lit(flow))
           )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Query UC API to get list of tables under selected schema

# COMMAND ----------

catalog_name, schema_name = schema_2L_name.split('.')
schema_response = requests.get(
    host + "/api/2.0/unity-catalog/tables",
    headers = {"Authorization": "Bearer " + token},
    params = {
      "catalog_name":catalog_name,
      "schema_name":schema_name
    }
)

if schema_response.status_code != requests.codes.ok:
    raise Exception(schema_response.json())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Programmatically retrieve the column lineage of each table, and write it out to the target

# COMMAND ----------

column_lineage_df = []

for table in schema_response.json()['tables']:
    table_name = table['full_name']

    # delete existing column lineages for this table
    spark.sql(f"DELETE FROM {lineage_table} WHERE table_name='{table_name}'")
  
    # retrieve column-level lineage for each column in this table
    for column in table['columns']:
        # query to get column-level lineage
        column_lineage = requests.get(
            host + "/api/2.0/lineage-tracking/column-lineage/get",
            headers={"Authorization": "Bearer " + token},
            params={
              "table_name": table_name, 
              "column_name": column['name']
            }
        )

      for flow in ['downstream_cols', 'upstream_cols']:
        # check that there is lineage information
        
          if flow in column_lineage:
              json_lineage = column_lineage.json()[flow]

              # read the json output into DataFrame
              df = spark.read.json(sc.parallelize([json_lineage]))
              # transform the json output
              df = format_lineage(flow, df)
              # add it to the list
              column_lineage_df.append(df)
    
# union all the lineage dataframes, and write to delta
if column_lineage_df:
    reduce(DataFrame.unionAll, column_lineage_df).write.mode("append").saveAsTable(lineage_table)

# COMMAND ----------

# example query from lineage table
display(spark.sql(f"SELECT * FROM {lineage_table}"))
