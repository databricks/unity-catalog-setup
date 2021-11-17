-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Delta Sharing Quickstart
-- MAGIC 
-- MAGIC <img src="https://raw.githubusercontent.com/databricks/tech-talks/master/images/delta_share_overview.png" width="700">
-- MAGIC 
-- MAGIC When using Delta sharing there are two important logical constructs to understand while using the service:
-- MAGIC - **Share** - is a logical collection containing a set of tables to be shared with the data recipient
-- MAGIC - **Recipient** - is a special entity representing the data recipient who is supposed to consume the shared data
-- MAGIC 
-- MAGIC One Unity Catalog Metastore can have multiple Shares and Recipients.
-- MAGIC 
-- MAGIC A data provider performs the following steps to achieve data sharing on Unity Catalog:
-- MAGIC 1. Create a Share and add data into it
-- MAGIC - Create a Recipient
-- MAGIC - Grant privileges of a Share to a Recipient
-- MAGIC - Send out activation link URL to Data Consumer (via email or instant messaging)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## READ ME FIRST
-- MAGIC - You must to be a metastore admin to create a share
-- MAGIC - This notebook must be run on DBR 8.4 and above (contains the Spark deltasharing connector)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Managing Shares
-- MAGIC 
-- MAGIC Shares are entities which represent a collection of tables intended to be shared as a group with a data recipient.
-- MAGIC 
-- MAGIC A share can be created by using the `CREATE SHARE` command.

-- COMMAND ----------

USE CATALOG quickstart_catalog;

CREATE SHARE IF NOT EXISTS quickstart_share

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Once you have created a share, the next step is to add the tables you want users to access to the Share.

-- COMMAND ----------

ALTER SHARE quickstart_share ADD TABLE quickstart_database.quickstart_table

-- COMMAND ----------

-- MAGIC %md
-- MAGIC You can also specify partition specification when adding a table to a Share, to share table’s data by its pre-defined partitions.

-- COMMAND ----------

ALTER SHARE quickstart_share ADD TABLE quickstart_database.quickstart_table
PARTITION (columnA = "1") AS quickstart_database.quickstart_table_partition_1

-- COMMAND ----------

-- MAGIC %md
-- MAGIC The `DESCRIBE SHARE` command can be used to view all tables inside of a given Share.

-- COMMAND ----------

-- View tables assigned to a specific share
SHOW ALL IN SHARE quickstart_share

-- COMMAND ----------

-- View metadata of a specific share
DESCRIBE SHARE quickstart_share

-- COMMAND ----------

-- MAGIC %md
-- MAGIC The `SHOW SHARES` command can be used to list all Shares under the current Metastore.

-- COMMAND ----------

-- List all the Shares under the Unity Catalog Metastore
SHOW SHARES

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Managing Recipients
-- MAGIC Recipients represent the end users who will be consuming the shared data. They are stored as separate entities from the Shares, and are granted access to specific Shares.
-- MAGIC 
-- MAGIC Recipients can be created using the `CREATE RECIPIENT` command. You can add comments for each Recipient as you create them.

-- COMMAND ----------

-- Create a recipient with a comment
CREATE RECIPIENT IF NOT EXISTS quickstart_recipient COMMENT "Intended recipient for of Delta Share"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC After creating a Recipient entity a unique activation URL will be generated. This URL will lead to a website for data recipients to download the credentials they will need to access the Share. Notice the **activation-link** field in the table displayed after you created the Recipient above. 
-- MAGIC 
-- MAGIC You can view the activation URL after creation as well as whether it has been activated yet by using the `DESCRIBE RECIPIENT` command.

-- COMMAND ----------

DESCRIBE RECIPIENT quickstart_recipient

-- COMMAND ----------

-- MAGIC %md
-- MAGIC You can view all Recipient entities that have been created in the current metastore using the `SHOW RECIPIENTS` command.

-- COMMAND ----------

SHOW RECIPIENTS

-- COMMAND ----------

--- You can also drop a recipient
--- DROP RECIPIENT quickstart_recipient

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Grant Access
-- MAGIC 
-- MAGIC In order to actually share the datasets in a Share with your Recipients, you need to grant read permission to the target Recipients on specific Shares. 

-- COMMAND ----------

Grant SELECT ON SHARE quickstart_share TO RECIPIENT quickstart_recipient

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### View Recipients Permissions
-- MAGIC 
-- MAGIC The following code can be used to view all of the shares that have been granted to inividual recipients.

-- COMMAND ----------

SHOW GRANT TO RECIPIENT quickstart_recipient

-- COMMAND ----------

-- MAGIC %md
-- MAGIC You can check also check all Recipients that are granted permission to read a specific Share

-- COMMAND ----------

SHOW GRANT ON SHARE quickstart_share

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Share the Activation URL
-- MAGIC 
-- MAGIC The data will be ready to be shared after creating the Share, creating the Recipient, and granting the proper level of access. 
-- MAGIC 
-- MAGIC The recipient can then access the data using the credentials obtained from the activation URL associated with the Recipient entity. It is the responsibility of the data provider to securely share the activation URL with the intended recipient.
