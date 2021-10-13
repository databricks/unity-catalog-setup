# Databricks notebook source
# MAGIC %md
# MAGIC # Delta Sharing Recipient Quickstart
# MAGIC 
# MAGIC 
# MAGIC <img src="https://raw.githubusercontent.com/databricks/tech-talks/master/images/delta_sharing_recipient_demo.png" width="700">

# COMMAND ----------

# MAGIC %md
# MAGIC ### Installing Delta Sharing Library
# MAGIC 
# MAGIC The delta-sharing is available as a python package that can be installed via pip. <br>

# COMMAND ----------

# MAGIC %pip install delta-sharing

# COMMAND ----------

import delta_sharing

# COMMAND ----------

# MAGIC %md
# MAGIC ## Delta Sharing Credentials as a Recipient
# MAGIC 
# MAGIC When a new Recipient entity is created for a Delta Share an activation link for that recipient will be generated. That URL will lead to a website for data recipients to download a credential file that contains a long-term access token for that recipient. Following the link will be take the recipient to an activation page that looks similar to this:
# MAGIC 
# MAGIC <img src="https://raw.githubusercontent.com/databricks/tech-talks/master/images/kanonymity_share_activation.png" width=600>
# MAGIC 
# MAGIC 
# MAGIC From this site the .share credential file can be downloaded by the recipient. This file contains the information and authorization token needed to access the Share. The contents of the file will look similar to the following example.
# MAGIC 
# MAGIC 
# MAGIC <img src="https://raw.githubusercontent.com/databricks/tech-talks/master/images/delta_sharing_cred_file_3.png" width="800">
# MAGIC 
# MAGIC Due to the sensitive nature of the token, be sure to save it in a secure location and be careful when visualising or displaying the contents. 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Storing the Share File
# MAGIC 
# MAGIC Recipients should download the share file and store it in a secure location. In order to utilize the share file it will have to be uploaded to a location accessible to the cluster, such as S3 or ADLS.  
# MAGIC 
# MAGIC For this quickstart example a share file has been made available by Databricks on a publicly readable S3 bucket.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Connecting to a Share
# MAGIC 
# MAGIC By using the share file credentials recipients can establish a connection to the Delta Share. The process for establishing the connection includes the following steps:
# MAGIC 
# MAGIC 1. Client presents the query and credentials to the sharing server
# MAGIC - The server verifies whether the client is allowed to access the data, logs the request, and then determines which data to send back
# MAGIC - The server generates short-lived and pre-signed URLs that allow the client to read these Parquet files directly from the cloud provider (here S3)
# MAGIC 
# MAGIC <img src="https://raw.githubusercontent.com/databricks/tech-talks/master/images/delta_sharing_under_the_hood.png" width="800">
# MAGIC 
# MAGIC Recipients are able to view details about the tables that are shared with them and query the data in the tables using Spark, Pandas, and other methods.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### List the Tables in a Share
# MAGIC 
# MAGIC Delta sharing protocol groups shared tables into a share schema.

# COMMAND ----------

share_file_path = 'https://databricks-datasets-oregon.s3-us-west-2.amazonaws.com/delta-sharing/share/open-datasets.share'

# Create a SharingClient
client = delta_sharing.SharingClient(share_file_path)

# List all shared tables.
client.list_all_tables()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC It is possible to iterate through the list to view all of the tables along with their corresponding schemas and shares.

# COMMAND ----------

shares = client.list_shares()

for share in shares:
  schemas = client.list_schemas(share)
  for schema in schemas:
    tables = client.list_tables(schema)
    for table in tables:
      print(f'name = {table.name}, share = {table.share}, schema = {table.schema}')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Query the Shared Table Using Spark
# MAGIC 
# MAGIC Data shared through Delta Shares can be queried using the Spark connector. 
# MAGIC 
# MAGIC To access the shared data an HDFS compliant path to the shared credential files is required. This is combined with the schema and table name to form a URL which is used to access a shared table.  <br>
# MAGIC 
# MAGIC The expected format of the url is: <br>
# MAGIC >{profile_file}#{share_id}.{database}.{table}

# COMMAND ----------

share_file_path = 's3a://databricks-datasets-oregon/delta-sharing/share/open-datasets.share'
table_url = f"{share_file_path}#delta_sharing.default.lending_club"

# COMMAND ----------

# MAGIC %md
# MAGIC The URL for the table to be queried can be passed to the delta sharing client to load the data using Spark.

# COMMAND ----------

shared_df = delta_sharing.load_as_spark(table_url)

display(shared_df)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Data loaded from a share can be manipulated as a standard spark dataframe.

# COMMAND ----------

from pyspark.sql.functions import sum, col

display(shared_df.
        groupBy('addr_state').
        agg(sum('loan_amnt').alias('Total Loans ($)')).
        orderBy(col("Total Loans ($)").desc()))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC The spark reader can also be used to load shared data by using the "deltaSharing" format within a standard `spark.read` method. 

# COMMAND ----------

shared_df2 = spark.read.format('deltaSharing').load(table_url)

display(shared_df2.
        groupBy('addr_state').
        agg(sum('loan_amnt').alias('Total Loans ($)')).
        orderBy(col("Total Loans ($)").desc()))

# COMMAND ----------

# MAGIC %md
# MAGIC SQL syntax can also be used to query shares.

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS default.sql_table;
# MAGIC 
# MAGIC CREATE TABLE IF NOT EXISTS default.sql_table 
# MAGIC   USING deltaSharing 
# MAGIC   LOCATION "s3a://databricks-datasets-oregon/delta-sharing/share/open-datasets.share#delta_sharing.default.lending_club";
# MAGIC   
# MAGIC SELECT * FROM default.sql_table;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Query the Shared Table Using Pandas
# MAGIC 
# MAGIC Shared data can be accessed via a Pandas connector. <br>
# MAGIC 
# MAGIC The way to specify the location of profile file differs slightly between connectors. When using Pandas a properly constructed URL which contains the schema and table name being queried is required. <br>
# MAGIC 
# MAGIC The expected format of the url is: <br>
# MAGIC >{profile_file}#{share_id}.{database}.{table}

# COMMAND ----------

share_file_path = 'https://databricks-datasets-oregon.s3-us-west-2.amazonaws.com/delta-sharing/share/open-datasets.share'
table_url = f"{share_file_path}#delta_sharing.default.lending_club"

# Use delta sharing client to load data
pandas_df = delta_sharing.load_as_pandas(table_url)

pandas_df.head(10)

# COMMAND ----------


