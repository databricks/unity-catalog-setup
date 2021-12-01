-- Databricks notebook source
-- MAGIC %md 
-- MAGIC ### Specify Catalog

-- COMMAND ----------

-- Specify Catalog, only for UC-enabled cluster
--- USE CATALOG hive_metastore

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ### Specify Database Containing Audit Logs

-- COMMAND ----------

-- Specify Database Containing Audit Logs
USE audit_logs

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ### Show Tables within Audit Logs Database

-- COMMAND ----------

-- Show Tables within Audit Logs Database
SHOW TABLES

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ### Describe the Unity Catalog Table

-- COMMAND ----------

-- Describe the Unity Catalog Table
DESCRIBE audit_logs.unitycatalog

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ### What types of Actions are captured by the Audit Logs?

-- COMMAND ----------

-- What types of Actions are captured by the Audit Logs?
SELECT
  distinct actionName
from
  audit_logs.unitycatalog
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
  audit_logs.unitycatalog
group by
  actionName

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
  audit_logs.unitycatalog
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
  audit_logs.unitycatalog
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
  audit_logs.unitycatalog
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
  audit_logs.unitycatalog
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
  audit_logs.unitycatalog
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
  audit_logs.unitycatalog
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
  audit_logs.unitycatalog
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
  audit_logs.unitycatalog
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
  audit_logs.unitycatalog
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
  concat_ws('.', requestParams.share,  requestParams.`schema`,  requestParams.`name`) as tableName,
  count(requestId) as requestsNum
FROM
  audit_logs.unitycatalog
where
  actionName = "deltaSharingQueryTable"
group by
  1,
  2,
  3,
  4
order by 3 desc

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
  response
FROM
  audit_logs.unitycatalog
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
  audit_logs.unitycatalog
where
  actionName in ("generateTemporaryTableCredential")
group by
  1,
  2,
  3,
  4
order by
  2 desc
