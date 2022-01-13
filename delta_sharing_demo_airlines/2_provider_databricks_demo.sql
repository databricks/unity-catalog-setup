-- Databricks notebook source
-- DBTITLE 1,B2B Data Exchange with Delta Sharing
-- MAGIC %md
-- MAGIC To Illustrate let's consider us a company like **TripActions**, a Corporate Travel & Spend Management Platform. We have already adopted a <b> Delta Lakehouse Architecture </b> for servicing all of our data internally. A few of our largest partnered airlines, <b>American Airlines</b> & <b>Southwest</b> just let us know that they are looking to partner to add reward and recommendation programs to airline customers using TripActions based off performance (of which we track seperately dictated by the customer) to augment our flight data. In order to pilot this new feature they need daily data of scheduled and results of flights taking within TripActions.
-- MAGIC 
-- MAGIC This is not a new occurance for our business as we get asked many times for data exchange with our large customers looking to do more with controlling and monitoring costs for travel. Depending on the opportunity presented with respect to the impact it will deliver to our business will dictate what we priortize to facilitate that need. If it was **easier** and **less costly** we could be more flexible with our offerings but that isn't possible today, until <b> Delta Lake & Delta Sharing </b>.<br>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC In Delta Sharing, it all starts with a Delta Lake table registered in the Delta Sharing Server by the data provider. This is where access permissions are established as to whom may receive the data.
-- MAGIC <br>
-- MAGIC <br>
-- MAGIC <img src="https://raw.githubusercontent.com/databricks/tech-talks/master/images/delta_share_overview.png" width="700"> 

-- COMMAND ----------

-- DBTITLE 1,Scenario 1: The Data Provider is a Databricks Customer
-- MAGIC %md
-- MAGIC #### Unity Catalog will be your Entitlement Layer Managed by Databricks
-- MAGIC As a data provider, you can make your Unity Catalog Metastore act as a Delta Sharing Server and share data on Unity Catalog with other organizations.  
-- MAGIC These organizations can then access the data using open source Apache Spark or pandas on any computing platform (including, but not limited to, Databricks). <br> <br>
-- MAGIC <img src="https://i.ibb.co/QJny676/Screen-Shot-2021-11-16-at-10-46-49-AM.png" width="600" height="480" /><br>
-- MAGIC                                                                                                 

-- COMMAND ----------

--REMOVE WIDGET SHARE_1;
--REMOVE WIDGET SHARE_2;
--REMOVE WIDGET RECIPIENT_1;
--REMOVE WIDGET RECIPIENT_2;
--CREATE WIDGET TEXT SHARE_1 DEFAULT "";
--CREATE WIDGET TEXT  SHARE_2 DEFAULT "";
--CREATE WIDGET TEXT RECIPIENT_1 DEFAULT "";
--CREATE WIDGET TEXT RECIPIENT_2 DEFAULT "";

-- COMMAND ----------

-- RUN THIS CELL PRIOR TO DEMO For Re-Execution 
-- make sure you are using a delta sharing enabled workspace & a UC enabled Cluster
DROP RECIPIENT IF EXISTS americanairlines;
DROP RECIPIENT IF EXISTS southwestairlines;
DROP SHARE IF EXISTS americanairlines;
DROP SHARE IF EXISTS southwestairlines;

-- COMMAND ----------

-- DBTITLE 1,Unity Catalog’s security model is based on standard ANSI SQL, to grant permissions at the level of databases, tables, views, rows and columns 
USE CATALOG deltasharing

-- COMMAND ----------

-- DBTITLE 1,Step 1: Create a Share Under The Unity Catalog Metastore
CREATE SHARE IF NOT EXISTS americanairlines COMMENT 'Daily Flight Data provided by Tripactions to American Airlines for Extended Rewards';
CREATE SHARE IF NOT EXISTS southwestairlines COMMENT 'Daily Flight Data provided by Tripactions to Southwest Airlines for Extended Rewards';

-- COMMAND ----------

-- DBTITLE 1,View a Share’s Metadata
DESCRIBE SHARE americanairlines;
DESCRIBE SHARE southwestairlines

-- COMMAND ----------

-- DBTITLE 1,Step 2: Add Tables to Share
ALTER SHARE americanairlines ADD TABLE airlinedata.lookupcodes;
ALTER SHARE southwestairlines ADD TABLE airlinedata.lookupcodes

-- COMMAND ----------

-- DBTITLE 1,Get In-House Unique Carrier Codes to Filter Specifically to Only the Relevant Airlines
SELECT
*
FROM
airlinedata.lookupcodes 
WHERE Description = "Southwest Airlines Co." OR Description = "American Airlines Inc."

-- COMMAND ----------

-- MAGIC %md
-- MAGIC We Don't Need to Give all the Historical Flights to Each Airline. Regardless of our UniqueCodes it's costly to each of our consumers and unnecessary to share more data then requested. 
-- MAGIC <br>
-- MAGIC ###Partition Specification & Renaming Tables with Alias for Customized Consumer Experience
-- MAGIC Specifying partition specification when adding a table to a Share allows you to share table’s data by its pre-defined partitions. Below is an example of sharing partial data in the `flights` table: 1) all data filter to each specific carrier, 2) all data for 2021 using the LIKE operator in one

-- COMMAND ----------

-- DBTITLE 1,Step 3: Add Partition Filter to Only Share a Portion of the Flights Table Using "=" & LIKE Operators AND Customized Aliases
ALTER SHARE americanairlines ADD TABLE deltasharing.airlinedata.flights PARTITION (UniqueCarrier = "AA", year LIKE "2008%") as airlinedata.`2008_flights`;
ALTER SHARE southwestairlines ADD TABLE deltasharing.airlinedata.flights PARTITION (UniqueCarrier = "WN")

-- COMMAND ----------

-- DBTITLE 1,Step 4: Remove Tables from Shares that are Unnecessary
--Remove table from the share
ALTER SHARE americanairlines REMOVE TABLE deltasharing.airlinedata.lookupcodes;
ALTER SHARE southwestairlines REMOVE TABLE deltasharing.airlinedata.lookupcodes;

-- COMMAND ----------

-- DBTITLE 1,Step 5: Display all Tables Inside a Share
SHOW ALL IN SHARE southwestairlines;
SHOW ALL IN SHARE americanairlines

-- COMMAND ----------

-- DBTITLE 1,Step 6: Create a Recipient(s)
CREATE RECIPIENT southwestairlines;
CREATE RECIPIENT americanairlines

-- COMMAND ----------

-- DBTITLE 1,Step 7: Activation link can be shared with recipient
-- MAGIC %md
-- MAGIC <img src="https://raw.githubusercontent.com/databricks/tech-talks/master/images/kanonymity_share_activation.png" width=600>

-- COMMAND ----------

-- DBTITLE 1,Download Share Profile & Store on DBFS - American Airlines
-- MAGIC %python
-- MAGIC import urllib.request
-- MAGIC sql("""DROP RECIPIENT IF EXISTS americanairlines""")
-- MAGIC df = sql("""CREATE RECIPIENT americanairlines""")
-- MAGIC link = df.collect()[0][4].replace('delta_sharing/retrieve_config.html?','api/2.0/unity-catalog/public/data_sharing_activation/')
-- MAGIC urllib.request.urlretrieve(link, "/tmp/americanairlines.share")
-- MAGIC dbutils.fs.mv("file:/tmp/americanairlines.share", "dbfs:/FileStore/americanairlines.share")

-- COMMAND ----------

-- DBTITLE 1,Download Share Profile & Store on DBFS - Southwest Airlines
-- MAGIC %python
-- MAGIC import urllib.request
-- MAGIC df = sql("""DESCRIBE RECIPIENT southwestairlines""")
-- MAGIC link = df.collect()[0][4].replace('delta_sharing/retrieve_config.html?','api/2.0/unity-catalog/public/data_sharing_activation/')
-- MAGIC urllib.request.urlretrieve(link, "/tmp/southwestairlines.share")
-- MAGIC dbutils.fs.mv("file:/tmp/southwestairlines.share", "dbfs:/FileStore/southwestairlines.share")

-- COMMAND ----------

-- DBTITLE 1,Step 8: Define which Data to Share, and Level of Access 
--'SELECT' gives read only permissions on all tables in the share
GRANT SELECT ON SHARE americanairlines TO RECIPIENT americanairlines;
GRANT SELECT ON SHARE southwestairlines TO RECIPIENT southwestairlines

-- COMMAND ----------

-- DBTITLE 1,Step 9: Audit Who has Access to a Share
SHOW GRANT ON SHARE southwestairlines;
SHOW GRANT ON SHARE americanairlines;

-- COMMAND ----------

-- DBTITLE 1,Step 10: Audit recipients
DESCRIBE RECIPIENT southwestairlines;
DESCRIBE RECIPIENT americanairlines;

-- COMMAND ----------

-- DBTITLE 1,Audit Recipient Level of Access
SHOW GRANT TO RECIPIENT southwestairlines;
SHOW GRANT TO RECIPIENT americanairlines;

-- COMMAND ----------

-- DBTITLE 1,Revoke Access if Needed
REVOKE SELECT ON SHARE southwestairlines FROM RECIPIENT southwestairlines

-- COMMAND ----------

-- DBTITLE 1,View Shares
SHOW ALL IN SHARE southwestairlines;
SHOW ALL IN SHARE americanairlines

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC 
-- MAGIC ## [Databricks Data Receiver Demo]($./3_receiver_databricks_demo)
