# Databricks notebook source
# MAGIC %md
# MAGIC ### Use Security Mode in the cluster creation UI for clusters with access to Unity Catalog
# MAGIC 
# MAGIC - **User isolation** - This provides a cluster that can use SQL only, but can be shared by multiple users
# MAGIC - **Single User** - This provides a cluster that supports multiple languages (SQL, python, scala, R), but one user must be nominated to use it exclusively (note that if a runtime version lower than DBR10.0 is selected, this mode will activate a passthrough cluster which will not have access to Unity Catalog).

# COMMAND ----------

# MAGIC %md
# MAGIC ### Switch to Preview channel to create a SQL endpoint with access to Unity Catalog
# MAGIC 
# MAGIC - Under the ‘Advanced Settings’ select the ‘Preview Channel’ when creating a SQL endpoint
