-- Databricks notebook source
-- MAGIC %md 
-- MAGIC ### Show Tables within Audit Logs Database

-- COMMAND ----------

CREATE WIDGET TEXT 0_catalog DEFAULT "audit_logs";

CREATE WIDGET TEXT 0_database DEFAULT "aws";

-- COMMAND ----------

-- Show Tables within Audit Logs Database
USE $0_catalog.$0_database;
SHOW TABLES

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ### Describe the Unity Catalog Table

-- COMMAND ----------

-- Describe the Unity Catalog Table
DESCRIBE unitycatalog

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ### What types of Actions are captured by the Audit Logs?

-- COMMAND ----------

-- What types of Actions are captured by the Audit Logs?
SELECT
  distinct actionName
from
  unitycatalog
order by
  actionName

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC 
-- MAGIC ### What are the most popular Actions?

-- COMMAND ----------

-- What are the most popular Actions?
SELECT
  actionName,
  count(actionName) as actionCount
from
  unitycatalog
group by
  actionName

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Unity Catalog audit logs

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ### Tracking UC Table Query Requests

-- COMMAND ----------

SELECT
  date_time,
  email,
  actionName,
  requestParams.operation as operation,
  requestParams.is_permissions_enforcing_client as pe_client,
  requestParams.table_full_name as table_name,
  response.errorMessage as error
FROM
  unitycatalog
where
  actionName in ("generateTemporaryTableCredential")
order by
  date_time desc

-- COMMAND ----------

SELECT
  email,
  date,
  requestParams.operation as operation,  
  requestParams.table_full_name as table_name,
  count(actionName) as queries
FROM
  unitycatalog
where
  actionName in ("generateTemporaryTableCredential")
group by
  1,
  2,
  3,
  4
order by
  2 desc

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ### Find out who Created, Updated and Deleted Delta Shares

-- COMMAND ----------

-- Find out who Created, Updated and Deleted Delta Shares
SELECT
  email,
  date_time,
  actionName,
  requestParams.name,
  requestParams.updates,
  requestParams.changes,
  response.result
FROM
  unitycatalog
WHERE
  actionName LIKE "%Share"
  OR actionName = "getActivationUrlInfo"
  OR actionName = "updateSharePermissions"

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ### Which Users are Most Active Overall?

-- COMMAND ----------

-- Which Users are Most Active Overall?
SELECT
  email,
  count(actionName) AS numActions
FROM
  unitycatalog
group by
  email
order by
  numActions desc

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ### Which Users are the Most Active Sharers?

-- COMMAND ----------

-- Which Users are the Most Active Sharers?
SELECT
  email,
  count(actionName) AS numActions
FROM
  unitycatalog
WHERE
  actionName like '%Share'
group by
  email
order by
  numActions desc

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ### Find out who Created and Retrieved Recipients

-- COMMAND ----------

-- Find out who Created and Retrieved Recipients
SELECT
  email,
  date_time,
  actionName,
  requestParams.name
FROM
  unitycatalog
where
  actionName Like "%Recipient"

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ### Who are the Delta Sharing Recipients? 

-- COMMAND ----------

-- Who are the Delta Sharing Recepients?
SELECT
  requestParams.name,
  count(requestParams.name) AS numActions
FROM
  unitycatalog
where
  actionName Like "%Recipient"
group by
  requestParams.name
order by
  numActions desc

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ### Audit all Delta Sharing Activities

-- COMMAND ----------

-- Audit all Delta Sharing Activities
SELECT
  requestParams.recipient_name,
  date_time,
  requestParams.share,
  requestParams.schema,
  actionName,
  requestParams
FROM
  unitycatalog
where
  requestParams.share is not null

-- COMMAND ----------

select
  requestParams.recipient_name,
  sourceIPAddress,
  actionName,
  date,
  concat_ws('.', requestParams.share,  requestParams.`schema`,  requestParams.`name`) as tableName,
  count(requestId)
from
  unitycatalog
where
  requestParams.share is not null
group by
  1,
  2,
  3,
  4,
  5
order by
  4 desc

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ### Tracking Usage of Delta Sharing (external shares) vs Unity Catalog (internal shares)

-- COMMAND ----------

-- Tracking Usage of Delta Sharing (external shares) vs Unity Catalog (internal shares)?
SELECT
  actionName,
  count(actionName) as numActions
FROM
  unitycatalog
where
  actionName Like "%deltaSharing%"
group by
  actionName
order by
  numActions desc

-- COMMAND ----------

-- Tracking Usage of Delta Sharing (external shares) vs Unity Catalog (internal shares)?
SELECT
  actionName,
  count(actionName)
FROM
  unitycatalog
where
  actionName not Like "%deltaSharing%"
group by
  actionName
order by
  count(actionName) desc

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ### Tracking Unity Catalog & Delta Sharing Activity by Date 

-- COMMAND ----------

-- Tracking Unity Catalog & Delta Sharing Activity by Date
SELECT
  count(actionName),
  to_date(date_time) as date
from
  unitycatalog
group by
  to_date(date_time)
order by
  date

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ### Tracking Delta Sharing Table Query Requests

-- COMMAND ----------

-- Tracking Delta Sharing Table Query Requests
SELECT
  requestParams.recipient_name,
  sourceIPAddress,
  date,
  actionName,
  concat_ws(
    '.',
    requestParams.share,
    requestParams.`schema`,
    requestParams.`name`
  ) as tableName,
  count(requestId) as numRequests
FROM
  unitycatalog
WHERE
  actionName like "deltaSharingQuer%"
group by
  1,
  2,
  3,
  4,
  5
order by
  3 desc

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Tracking Shared Data Volume from Recipient

-- COMMAND ----------

select
  recipient_name,
  date,
  sum(numBytes) as numBytes,
  sum(numRecords) as numRecords,
  sum(numFiles) as numFiles
from
  (
    select
      requestParams.recipient_name as recipient_name,
      date,
      CAST(
        from_json(response.result, 'scannedAddFileSize STRING').scannedAddFileSize AS INT
      ) as numBytes,
      CAST(
        from_json(response.result, 'numRecords STRING').numRecords AS INT
      ) as numRecords,
      CAST(
        from_json(response.result, 'activeAddFiles STRING').activeAddFiles AS INT
      ) as numFiles,
      actionName
    from
      unitycatalog
    where
      requestParams.recipient_name is not null
      and actionName = "deltaSharingQueriedTable"
  )
group by
  recipient_name,
  date
order by
  numBytes desc;
