# Databricks notebook source
# MAGIC %md
# MAGIC ## Collect column-level lineage and populate a Delta table
# MAGIC 
# MAGIC **Note**: UC Lineage is currently in Private Preview
# MAGIC 
# MAGIC This notebook exports column-level lineage from all UC tables under a catalog (or all catalogs by specifying `<all>` - this might take a long time)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Provide parameters in the widgets
# MAGIC 
# MAGIC Need to provide 2 parameters:
# MAGIC - Name of the catalog to extract lineage from
# MAGIC - The 3L name for the new target Delta table where the column-level lineage will be exported (catalog & schema should already exist, with the appropriate permissions to create a new table)

# COMMAND ----------

dbutils.widgets.removeAll()

# COMMAND ----------

dbutils.widgets.text("Catalog name (specify <all> for all catalogs)", "<all>")
dbutils.widgets.text("Target lineage table (3L)", "vuongnguyen.default.column_lineage")

# COMMAND ----------

catalog_name: str = dbutils.widgets.get("Catalog name (specify <all> for all catalogs)")
lineage_table: str = dbutils.widgets.get("Target lineage table (3L)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prepare import

# COMMAND ----------

import requests
from requests.sessions import Session
import json
from pyspark.sql.functions import explode, lit, concat_ws
from functools import reduce
from pyspark.sql import DataFrame

# COMMAND ----------

host: str = (
    dbutils.notebook.entry_point.getDbutils()
    .notebook()
    .getContext()
    .apiUrl()
    .getOrElse(None)
)
token: str = (
    dbutils.notebook.entry_point.getDbutils()
    .notebook()
    .getContext()
    .apiToken()
    .getOrElse(None)
)

# COMMAND ----------

# prepare a request session to reuse
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

retry_strategy = Retry(
    total=3,
    backoff_factor=1,
    status_forcelist=[429, 500, 502, 503, 504],
    method_whitelist=["HEAD", "GET", "OPTIONS"],
)
adapter = HTTPAdapter(max_retries=retry_strategy)
http = requests.Session()
http.mount("https://", adapter)
http.mount("http://", adapter)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prepare target table to store lineage

# COMMAND ----------

## create the delta table to store the column-level lineage
spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS {lineage_table} (
      table_name STRING,
      column_name STRING,
      flow STRING,
      lineage_table_name STRING,
      lineage_column_name STRING,
      table_type STRING,
      workspace_id LONG
    ) USING DELTA COMMENT 'This is my column lineage information'
      """
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define function to query UC APIs

# COMMAND ----------


def query_uc_api(session: Session, endpoint: str, params: dict) -> dict:

    query = session.get(
        host + f"/api/2.0/{endpoint}",
        headers={"Authorization": "Bearer " + token},
        params=params,
    )
    if query.status_code != requests.codes.ok:
        raise Exception(query.json())
    return query.json()


# COMMAND ----------

# MAGIC %md
# MAGIC ## Query UC API to get list of schemas

# COMMAND ----------

catalogs = [
    catalog["name"]
    for catalog in query_uc_api(http, "unity-catalog/catalogs", {})["catalogs"]
]

if catalog_name == "<all>":
    # delete existing column lineages for this table
    spark.sql(f"DELETE FROM {lineage_table}")
else:
    if catalog_name not in catalogs:
        raise Exception("Catalog does not exist")
    catalogs = [catalog_name]
    spark.sql(f"DELETE FROM {lineage_table} WHERE table_name LIKE '{catalog_name}.%'")

# COMMAND ----------

all_schemas = []
for catalog in catalogs:
    # need to skip certain catalogs, e.g. Delta Sharing shares logged as catalogs
    try:
        schemas = [
            (catalog, schema["name"])
            for schema in query_uc_api(
                http, "unity-catalog/schemas", {"catalog_name": catalog}
            )["schemas"]
        ]
        all_schemas.extend(schemas)
    except:
        pass

# COMMAND ----------

# MAGIC %md
# MAGIC ## Programmatically retrieve the column lineage of each table, and write it out to the target

# COMMAND ----------

# extract lineage information from the json output
def format_lineage(
    flow: str, column_lineage: DataFrame, table_name: str, col_name: str
) -> DataFrame:
    return (
        column_lineage.withColumnRenamed("name", "lineage_column_name")
        .withColumn(
            "lineage_table_name",
            concat_ws(".", "catalog_name", "schema_name", "table_name"),
        )
        .drop("catalog_name", "schema_name", "table_name")
        .withColumn("table_name", lit(table_name))
        .withColumn("column_name", lit(col_name))
        .withColumn("flow", lit(flow))
    )


# COMMAND ----------

column_lineage_df: List[DataFrame] = []

for catalog, schema in all_schemas:
    tables_query = query_uc_api(
        http, "unity-catalog/tables", {"catalog_name": catalog, "schema_name": schema}
    )
    if "tables" in tables_query:
        for table in tables_query["tables"]:
            table_name = table["full_name"]

            # retrieve column-level lineage for each column in this table
            if "columns" in table:
                for column in table["columns"]:
                    # query to get column-level lineage
                    column_lineage = query_uc_api(
                        http,
                        "lineage-tracking/column-lineage/get",
                        {
                            "table_name": table_name,
                            "column_name": column["name"],
                        },
                    )

                    for flow in ["downstream_cols", "upstream_cols"]:
                        # check that there is lineage information
                        if flow in column_lineage:
                            json_lineage = column_lineage[flow]

                            # read the json output into DataFrame
                            df = spark.read.json(sc.parallelize([json_lineage]))
                            # transform the json output
                            df = format_lineage(flow, df, table_name, column["name"])
                            # add it to the list
                            column_lineage_df.append(df)

# union all the lineage dataframes, and write to delta
if column_lineage_df:
    reduce(DataFrame.unionAll, column_lineage_df).write.mode("append").saveAsTable(
        lineage_table
    )

# COMMAND ----------

# getting column lineage
display(spark.sql(f"SELECT * FROM {lineage_table}"))

# COMMAND ----------

# getting table lineage by doing select distinct
display(
    spark.sql(
        f"SELECT DISTINCT table_name, flow, lineage_table_name, table_type, workspace_id FROM {lineage_table}"
    )
)
