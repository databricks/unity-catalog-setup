-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Unity Catalog Quickstart (SQL)
-- MAGIC 
-- MAGIC A notebook that provides an example workflow for using the Unity Catalog. Steps include:
-- MAGIC 
-- MAGIC - Choosing a catalog and creating a new schema
-- MAGIC - Creating a managed and unmanaged table and adding it to the schema
-- MAGIC - Querying the tables using the catalog namespace conventions
-- MAGIC - Managing data access permissions on the tables

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Namespace
-- MAGIC 
-- MAGIC In the previous setup notebook you created a metastore catalog. Within a metastore, Unity Catalog provides a three-level namespace for organizing data: catalogs, schemas (also called databases), and tables / views
-- MAGIC 
-- MAGIC >`Catalog.Schema.Table`
-- MAGIC 
-- MAGIC For Databricks users who already use the Apache Hive metastore available in each workspace, or an external Hive metastore, Unity Catalog is **additive**: the workspace’s Hive metastore becomes one catalog within the 3-layer namespace (called “legacy”), and other catalogs use Unity Catalog.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Create New Catalog
-- MAGIC 
-- MAGIC After the initial setup of Unity Catalog a default catalog named 'main' will be created along with an empty schema 'default'. 
-- MAGIC 
-- MAGIC New catalogs can be created using the `CREATE CATALOG` command.

-- COMMAND ----------

--- create a new catalog
CREATE CATALOG IF NOT EXISTS quickstart_catalog

-- COMMAND ----------

-- Set the current catalog
USE CATALOG quickstart_catalog

-- COMMAND ----------

--- Show all existing catalogs
SHOW CATALOGS

-- COMMAND ----------

--- grant create & usage permissions to all users on the account
--- this also works for other account-level groups and individual users
GRANT CREATE,
USAGE on CATALOG quickstart_catalog TO `account users`

-- COMMAND ----------

--- check that the grants are correct on the quickstart catalog
SHOW GRANT on CATALOG quickstart_catalog

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Create and View Schemas
-- MAGIC Schemas, also referred to as Databases, are the second layer of the Unity Catalog namespace. They can be used to logically organize tables. 

-- COMMAND ----------

--- create a new schema under the quick start catalog
CREATE SCHEMA IF NOT EXISTS quickstart_database COMMENT "A new Unity Catalog schema called quickstart_database"

-- COMMAND ----------

-- Show schemas under catalog
SHOW SCHEMAS

-- COMMAND ----------

-- Describe the new schema
DESCRIBE SCHEMA EXTENDED quickstart_database

-- COMMAND ----------

-- Drop a schema
-- DROP SCHEMA quickstart_database CASCADE

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Create a Managed Table
-- MAGIC 
-- MAGIC *Managed tables* are the default way to create table with Unity Catalog. If no specific path statement is included, a database or table is created within the managed storage (storage root) that was setup when you created  the metastore.

-- COMMAND ----------

-- Set the current schema
USE quickstart_database

-- COMMAND ----------

-- Create managed Delta table
CREATE TABLE IF NOT EXISTS quickstart_table (columnA Int, columnB String) PARTITIONED BY (columnA);
INSERT INTO
  TABLE quickstart_table
VALUES
  (1, "one"),
  (2, "two")

-- COMMAND ----------

-- View all tables in the schema
SHOW TABLES IN quickstart_database

-- COMMAND ----------

-- Describe this table
DESCRIBE TABLE EXTENDED quickstart_table

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC With the three level namespaces you can access tables in several different ways:
-- MAGIC - Access the table with a fully qualified name
-- MAGIC - Select a default catalog and access the table using the database/schema and the name
-- MAGIC - Select a default database/schema and use the table name
-- MAGIC 
-- MAGIC The following three commands are functionally equivalent-

-- COMMAND ----------

-- Query the table using the three part namespace
SELECT
  *
FROM
  quickstart_catalog.quickstart_database.quickstart_table

-- COMMAND ----------

-- Set the default catalog and query the table using the schema and table name
USE CATALOG quickstart_catalog;
SELECT
  *
FROM
  quickstart_database.quickstart_table

-- COMMAND ----------

-- Set the default catalog and default schema and query the table using the table name
USE CATALOG quickstart_catalog;
USE quickstart_database;
SELECT
  *
FROM
  quickstart_table

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## Create an External Table
-- MAGIC 
-- MAGIC *External Tables* are tables created outside of the managed storage location, and are not fully managed by the Unity Catalog.  The primary difference between an external table and a managed table is that dropping an external table from the catalog does not delete the underlying data files. 
-- MAGIC 
-- MAGIC External tables are metadata definitions stored in the metastore, and benefit from the same governance model applied to managed tables. 

-- COMMAND ----------

USE CATALOG quickstart_catalog;
CREATE TABLE IF NOT EXISTS quickstart_database.city_data (
  rankIn2016 INT,
  state STRING,
  stateAbbrev STRING,
  population2010 LONG,
  estPopulation2016 LONG,
  city STRING
) USING Delta LOCATION "s3://databricks-corp-training/common/City-Data.delta";
SELECT
  *
FROM
  quickstart_database.city_data;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Drop Table
-- MAGIC If a *managed table* is dropped with the `DROP TABLE` command, the underlying data files are removed as well. 
-- MAGIC 
-- MAGIC If an *unmanaged table* is dropped with the `DROP TABLE` command, the metadata about the table is removed from the catalog but the underlying data files are not deleted. 

-- COMMAND ----------

-- Drop the unmanaged table
DROP TABLE quickstart_database.city_data

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## Migrate Existing Tables to Unity Catalog
-- MAGIC 
-- MAGIC Existing tables stored within a workspaces metastore from before the introduction of Unity Catalog are still available for use and can be accessed from a catalog named `hive_metastore`. 
-- MAGIC 
-- MAGIC The existing tables from the legacy metastore can be migrated to the Unity Catalog using a pattern similar to the one below.

-- COMMAND ----------

USE CATALOG quickstart_catalog;
CREATE TABLE quickstart_database.migrated_table AS
SELECT
  *
FROM
  hive_metastore.{ database_name }.{ table_name };

-- COMMAND ----------

-- MAGIC %md
-- MAGIC If the legacy table is a Delta table, you can create a deep clone of the source table to preserve the Delta log. 

-- COMMAND ----------

USE CATALOG quickstart_catalog;
CREATE TABLE IF NOT EXISTS quickstart_database.migrated_table DEEP CLONE hive_metastore.default.city_data

-- COMMAND ----------

-- MAGIC %md
-- MAGIC You can also migrate external tables from the legacy metastore by recreating the tables in the new Unity Catalog.
-- MAGIC 
-- MAGIC The location of an existing external table can be found using the `DESCRIBE TABLE EXTENDED` command. Once you know the location you can use it to add the table to the new Unity Catalog metastore.

-- COMMAND ----------

-- Create example external table in legacy store
USE CATALOG hive_metastore;
CREATE TABLE IF NOT EXISTS default.external_table (
  rankIn2016 INT,
  state STRING,
  stateAbbrev STRING,
  population2010 LONG,
  estPopulation2016 LONG,
  city STRING
) USING PARQUET LOCATION "s3a://databricks-corp-training/common/City-Data.parquet";

-- COMMAND ----------

-- Describe the external table and find the Location
DESCRIBE TABLE EXTENDED default.external_table

-- COMMAND ----------

--Create the external table in the Unity Catalog
USE CATALOG quickstart_catalog;
CREATE TABLE quickstart_database.migrated_external_table (
  rankIn2016 INT,
  state STRING,
  stateAbbrev STRING,
  population2010 LONG,
  estPopulation2016 LONG,
  city STRING
) USING PARQUET LOCATION "s3://databricks-corp-training/common/City-Data.parquet";

-- COMMAND ----------

USE CATALOG hive_metastore;
DROP TABLE default.external_table

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Managing Permissions on Data
-- MAGIC 
-- MAGIC Data access control lets you grant and revoke access to your data. The Unity Catalog is a “shall grant” secure-by-default architecture. Initially, all users have no access to data.  Only administrators have access to data, and can grant/revoke access to users/groups on specific data assets.
-- MAGIC 
-- MAGIC #### Ownership
-- MAGIC Every securables in Unity Catalog has an owner. The owner can be any account-level user or group, called *principals* in general. A principal becomes the owner of a securable when they create it. Pricipals are also used to manage permissions to data in the metastore.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Managing Privileges
-- MAGIC The following are the SQL privileges supported on Unity Catalog:
-- MAGIC - SELECT: Allows the grantee to read data from the securable.
-- MAGIC - MODIFY: Allows the grantee to add,  update and delete data to or from the securable.
-- MAGIC - CREATE: Allows the grantee to create child securables within this securable.
-- MAGIC - USAGE: Allows the grantee to read or modify (add/delete/update) the child securables within this securable.
-- MAGIC 
-- MAGIC Note that privileges are NOT inherited on the securables. This means granting a privilege on a securable DOES NOT automatically grant the privilege on its child securables.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Granting**: Grants a privilege on securable(s) to a principal. Only Metastore Admin and owners of the securable can perform privilege granting.

-- COMMAND ----------

-- Privileges can be granted to Schemas
GRANT USAGE ON SCHEMA quickstart_database TO `account users`

-- COMMAND ----------

-- Grants read privilege on the table to a principal
GRANT
SELECT
  ON TABLE quickstart_database.quickstart_table TO `account users`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Show Granting**: Lists all privileges that are granted on a securable.

-- COMMAND ----------

-- Lists all privileges that are granted on a securable
SHOW GRANTS ON TABLE quickstart_catalog.quickstart_database.quickstart_table

-- COMMAND ----------

SHOW GRANTS ON SCHEMA quickstart_catalog.quickstart_database

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Revoking**: Revokes a previously granted privilege on securable(s) from a principal.

-- COMMAND ----------

REVOKE
SELECT
  ON TABLE quickstart_database.quickstart_table
FROM
  `account users`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Troubleshooting Queries
