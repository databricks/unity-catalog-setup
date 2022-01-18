# Databricks notebook source
# MAGIC %md
# MAGIC ### Use the 2 json definition under cluster_policies folder to set up the relevant policies that enforce clusters creation with access to UC by default

# COMMAND ----------

# MAGIC %md
# MAGIC - `uc-sql-only.json` - enforces User isolation clusters, which supports **SQL only**, but can be shared by multiple users
# MAGIC - `uc-multi-lang.json` - enforces Single user clusters, which supports multiple languages (SQL, python, scala, R), but **one user** must be nominated to use it exclusively 
