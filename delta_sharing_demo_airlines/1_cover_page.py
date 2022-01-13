# Databricks notebook source
# MAGIC %md
# MAGIC ## The Opportunity: Easily Distribute Successful Data Products   <img src="https://i.ibb.co/vdNHFZN/deltasharingimage.png" width = "100"></a>
# MAGIC 
# MAGIC With Delta Sharing & Unity Catalog Organizations can Fully Realize the Opportunity to better Govern, Package, and Deliver their Historical and Reference data to both <b>Internal</b> and <b>External Customers.</b>

# COMMAND ----------

# DBTITLE 1,Delta Sharing: An Open Protocol for Secure Data Sharing
# MAGIC %md
# MAGIC  Delta Sharing is an open protocol for secure data sharing with other organizations regardless of which computing platforms they use. It can share large datasets in an S3 data lake in real time without copying them, so that data recipients can immediately begin working with the latest version of the shared data
# MAGIC <br>
# MAGIC <br>
# MAGIC <b>Highlights:</b>
# MAGIC - **Scale** to massive datasets
# MAGIC - Share **Live** data directly **No-Copying**
# MAGIC - Strong **security**, auditing and governance  
# MAGIC - **Simple** administration
# MAGIC - Direct **integration** with the rest of **Databricks** (In Progress)
# MAGIC - **No** compute 
# MAGIC </br> </br>
# MAGIC 
# MAGIC <img src="https://raw.githubusercontent.com/databricks/tech-talks/master/images/delta_share_overview.png" width="700">

# COMMAND ----------

# DBTITLE 1,Flexibility - Open Source, Vendor-Independent with a Fast Growing Community
# MAGIC %md
# MAGIC <img src="https://i.ibb.co/R0wsczm/Screen-Shot-2021-11-16-at-8-33-52-AM.png" width="1000"> 

# COMMAND ----------

# DBTITLE 1,Simple, Secure, Governance with Databricks Unity Catalog
# MAGIC %md
# MAGIC Unity Catalog as your Databricks hosted entitlement layer will provide a secure, scalable Delta Sharing service for you. 
# MAGIC 
# MAGIC Without Unity Catalog, data providers will need to host their own [Delta Sharing Server](https://github.com/delta-io/delta-sharing#delta-sharing-reference-server) </br> <img src=https://i.ibb.co/vVZMZVs/Screen-Shot-2021-11-16-at-8-13-41-AM.png width=800px></a>

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC ## [Databricks Data Provider Demo]($./2_provider_databricks_demo)   
# MAGIC ### use cluster : demo_deltasharing_multi_language_single_user

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC ## [Databricks Data Receiver Demo]($./3_receiver_databricks_demo)
# MAGIC ### use cluster: demo_deltasharing_reciever
