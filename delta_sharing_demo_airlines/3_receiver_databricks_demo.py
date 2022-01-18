# Databricks notebook source
# DBTITLE 1,Empower your Consumer and Expand your Market Footprint
# MAGIC %md
# MAGIC <img src="https://i.ibb.co/0JbrY2S/Screen-Shot-2021-11-16-at-10-07-58-AM.png" width="800"></a> <a href="https://ibb.co/Gtt2bw4">

# COMMAND ----------

# DBTITLE 1,Delta Sharing Credentials as a Recipient
# MAGIC %md
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

# DBTITLE 0,Let's Start With the Vast and Trusted Python Developers as Consumers
# MAGIC %md
# MAGIC #Let's Start With the Vast and Trusted Python Developers as Consumers (10M+ Users Globally)
# MAGIC delta-sharing is available as a python package that can be installed via pip. <br>
# MAGIC This simplifies the consumer side integration; anyone who can run python can consume shared data via SharingClient object. <br>

# COMMAND ----------

# DBTITLE 1,Step 0: Installing Delta Sharing library
# MAGIC %pip install delta-sharing

# COMMAND ----------

import delta_sharing

# COMMAND ----------

# DBTITLE 1,Use credentials to access the share and list tables available to me 
import urllib.request
urllib.request.urlretrieve("https://databricks-datasets-oregon.s3-us-west-2.amazonaws.com/delta-sharing/share/open-datasets.share", "/tmp/open_datasets.share")
dbutils.fs.mv("file:/tmp/open_datasets.share", "dbfs:/FileStore/open_datasets.share")

# COMMAND ----------

# MAGIC %md
# MAGIC After fetching and storing the share file, if we head the file via fs magic we can inspect the bearerToken <br>
# MAGIC Due to the sensitive nature of the token, please be careful when visualising it and where you are storing the share file. 

# COMMAND ----------

# American Airlines
profile_file = '/dbfs/FileStore/americanairlines.share'

# Create a SharingClient
client = delta_sharing.SharingClient(profile_file)

# List all shared tables.
client.list_all_tables()

# COMMAND ----------

# Southwest Airlines
southwest_profile = '/dbfs/FileStore/southwestairlines.share'

# Create a SharingClient
client = delta_sharing.SharingClient(southwest_profile)

# List all shared tables.
client.list_all_tables()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC It is possible to iterate through the list to view all of the tables along with their corresponding schemas and shares. <br>
# MAGIC The share file can be stored on a remote storage.

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
# MAGIC # Query the Shared Table Using the Ever so Popular Pandas

# COMMAND ----------

# MAGIC %md
# MAGIC Delta sharing allows us to access data via Pandas connector. <br>
# MAGIC To access the shared data we require a properly constructed url. <br>
# MAGIC The expected format of the url is: < profile_file \>#< share_id \>.< database \>.< table \><br>

# COMMAND ----------

profile_file = '/dbfs/FileStore/americanairlines.share'
table_url = f"{profile_file}#americanairlines.airlinedata.2008_flights"

# Use delta sharing client to load data
flights_df = delta_sharing.load_as_pandas(table_url)

flights_df.head(10)

# COMMAND ----------

# MAGIC %md
# MAGIC # Query using Spark (1000's of Organizations Globally)

# COMMAND ----------

# MAGIC %md
# MAGIC Similarly to Pandas connect delta sharing comes with a spark connector. <br>
# MAGIC The way to specify the location of profile file slightly differs between connectors. <br>
# MAGIC For spark connector the profile file path needs to be HDFS compliant. <br>

# COMMAND ----------

profile_file = '/FileStore/americanairlines.share'
table_url = f"{profile_file}#americanairlines.airlinedata.2008_flights"

# COMMAND ----------

# MAGIC %md
# MAGIC To load the data into spark, we can use delta sharing client.

# COMMAND ----------

spark_flights_df = delta_sharing.load_as_spark(table_url)

from pyspark.sql.functions import sum, col, count

display(spark_flights_df.
        where('cancelled = 1').
        groupBy('UniqueCarrier', 'month', 'year').
        agg(count('FlightNum').alias('Total Cancellations')).
        orderBy(col('year').asc(), col('month').asc(), col('Total Cancellations').desc()))

# COMMAND ----------

# MAGIC %md
# MAGIC Alternatively, we can use 'deltaSharing' fromat in spark reader. 

# COMMAND ----------

spark_flights_df = spark.read.format('deltaSharing').load(table_url)

display(spark_flights_df.
        where('cancelled = 1').
        groupBy('UniqueCarrier', 'month').
        agg(count('FlightNum').alias('Total Cancellations')).
        orderBy(col('month').asc(), col('Total Cancellations').desc()))

# COMMAND ----------

# MAGIC %md
# MAGIC # Empower Your Customer Base, Query as a SQL table, it's Your Perogative Users!
# MAGIC ### The most popular language, 10m+ users globally

# COMMAND ----------

# MAGIC %md 
# MAGIC We can create a SQL table and use `'deltaSharing'` as a datasource. <br>
# MAGIC We need to provide the url as: `< profile_file >#< share_id >.< database >.< table >` <br>
# MAGIC Note that in this case we cannot use secrets since other parties would be able to see the token in clear text via table properties.

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS delta_sharing_demo_flights

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS delta_sharing_demo_flights
# MAGIC     USING deltaSharing
# MAGIC     LOCATION "dbfs:/FileStore/americanairlines.share#americanairlines.airlinedata.2008_flights"

# COMMAND ----------

# MAGIC %md
# MAGIC #Join Partner Data with <b>Internal Data</b> for Additional Intelligence

# COMMAND ----------

# MAGIC %md
# MAGIC We can now treat this table as any other table accessible from spark sql. 

# COMMAND ----------

# DBTITLE 1,Combine Easily with Existing Internal Data for Immediate Insights - Top 25 Airports with Delays
# MAGIC %sql
# MAGIC SELECT
# MAGIC Origin as OriginCode,
# MAGIC   Dest as DestCode,
# MAGIC   concat(concat(a.city,","),a.state) as OriginAirport,
# MAGIC   concat(concat(b.city,","),b.state) as DestinationAirport,
# MAGIC   Avg(ArrDelay) AS avgArrDelay,
# MAGIC   Avg(DepDelay) AS avgDepDelay
# MAGIC FROM
# MAGIC     delta_sharing_demo_flights
# MAGIC   LEFT JOIN default.airports a on Origin = a.IATA
# MAGIC   LEFT JOIN default.airports b on Dest = b.IATA
# MAGIC     group by Dest,Origin,a.city, b.city, a.state,b.state
# MAGIC     order by avgArrDelay DESC,avgDepDelay DESC
# MAGIC     LIMIT 25

# COMMAND ----------

# MAGIC %md
# MAGIC #The Business Consumer who **LOVES** Power BI!
# MAGIC  ### The most popular business intelligence tool, 1m+ users globally

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="https://s1.gifyu.com/images/PBI_Deltasharingxlarge.gif" width="1200" height="480" /></a>

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC ## [Back to Overview]($./1_cover_page)
