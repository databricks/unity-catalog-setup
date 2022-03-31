# Databricks notebook source
# MAGIC %md
# MAGIC ### Create Filters - Audit Log Tables and Date Range

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE WIDGET TEXT 0_catalog DEFAULT "audit_logs";
# MAGIC 
# MAGIC CREATE WIDGET TEXT 0_database DEFAULT "aws";
# MAGIC 
# MAGIC CREATE WIDGET TEXT 1_Date_Begin DEFAULT "2021-11-24";
# MAGIC 
# MAGIC CREATE WIDGET TEXT 2_Date_End DEFAULT "2022-12-31";

# COMMAND ----------

# MAGIC %md
# MAGIC ### Select appropriate catalog & database

# COMMAND ----------

# MAGIC %sql
# MAGIC USE $0_catalog.$0_database

# COMMAND ----------

# MAGIC %md
# MAGIC ### Recipient details

# COMMAND ----------

recipients_df = spark.sql("SHOW RECIPIENTS")

recipients = []

for recipient in recipients_df.collect():
    recipient_name = recipient[0]
    detail = spark.sql(f"DESCRIBE RECIPIENT `{recipient_name}`")
    recipients.append(detail)

reduce(DataFrame.unionAll, recipients).createOrReplaceTempView("delta_sharing_recipients")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Current shares

# COMMAND ----------

from functools import reduce
from pyspark.sql import DataFrame
from pyspark.sql.functions import lit

shares_df = spark.sql("SHOW SHARES")

shares_df.createOrReplaceTempView("delta_sharing_shares")

grants = []
tables = []

for share in shares_df.collect():
    share_name = share[0]
    grant_df = spark.sql(f"SHOW GRANTS ON SHARE `{share_name}`")
    table_df = spark.sql(f"SHOW ALL IN SHARE `{share_name}`")
    
    grants.append(grant_df.select("*").withColumn("share", lit(share_name)))
    tables.append(table_df.select("*").withColumn("share", lit(share_name)))
    
    
reduce(DataFrame.unionAll, grants).createOrReplaceTempView("delta_sharing_grants")
reduce(DataFrame.unionAll, tables).createOrReplaceTempView("delta_sharing_tables")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   delta_sharing_tables

# COMMAND ----------

# MAGIC %md
# MAGIC ### Recipients Current Accessibility to Share(s)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   delta_sharing_grants
# MAGIC   JOIN delta_sharing_tables USING (share)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Recipient Information (Who and When it was created, current permissions, last modified, first & last use)

# COMMAND ----------

# MAGIC %sql
# MAGIC Select
# MAGIC   name as `Recipient`,
# MAGIC   created_by as `Created By`,
# MAGIC   `First Used`,
# MAGIC   `Last Used`,
# MAGIC   `Total` as `Total Actions`,  
# MAGIC   `Successful %`,
# MAGIC   `Total Successful`, 
# MAGIC   `Total Failed`,
# MAGIC   `activation_link` as `Activation Link`,
# MAGIC   active_token_id as `Active Token ID`,
# MAGIC   date_format(active_token_expiration_time, "yyyy-mm-dd") as `Active Token Expiration`,
# MAGIC   rotated_token_id as `Rotated Token`,
# MAGIC   rotated_token_expiration_time as `Rotated Token Expiration`,
# MAGIC   comment as `Comment`
# MAGIC FROM
# MAGIC   delta_sharing_recipients r
# MAGIC   LEFT JOIN (
# MAGIC     SELECT
# MAGIC       requestParams.recipient_name as recipient_name,
# MAGIC       MIN(date) as `First Used`,
# MAGIC       MAX(date) as `Last Used`,
# MAGIC       format_number(
# MAGIC         (
# MAGIC           SUM(
# MAGIC             (
# MAGIC               CASE
# MAGIC                 WHEN response.statusCode = "200" THEN 1
# MAGIC                 ELSE 0
# MAGIC               END
# MAGIC             )
# MAGIC           ) / count(lower(actionName))
# MAGIC         ),
# MAGIC         "#%"
# MAGIC       ) as `Successful %`,
# MAGIC       SUM(
# MAGIC         (
# MAGIC           CASE
# MAGIC             WHEN response.statusCode <> "200" THEN 1
# MAGIC             ELSE 0
# MAGIC           END
# MAGIC         )
# MAGIC       ) as `Total Failed`,
# MAGIC       SUM(
# MAGIC         (
# MAGIC           CASE
# MAGIC             WHEN response.statusCode = "200" THEN 1
# MAGIC             ELSE 0
# MAGIC           END
# MAGIC         )
# MAGIC       ) as `Total Successful`,
# MAGIC       count(lower(actionName)) as `Total`
# MAGIC     from
# MAGIC       unitycatalog
# MAGIC     group by
# MAGIC       requestParams.recipient_name
# MAGIC     order by
# MAGIC       `Total` desc
# MAGIC   ) u on u.recipient_name = r.name

# COMMAND ----------

# MAGIC %md
# MAGIC ### Total Activity and Failure Ratio per Recipient

# COMMAND ----------

# MAGIC %sql --last week or last two weeks (what you care about for failures)
# MAGIC SELECT
# MAGIC   requestParams.recipient_name,
# MAGIC   email,
# MAGIC   lower(actionName) as `Actions`,
# MAGIC   MIN(date) as `First Used`,
# MAGIC   MAX(date) as `Last Used`,
# MAGIC   format_number(
# MAGIC     (
# MAGIC       SUM(
# MAGIC         (
# MAGIC           CASE
# MAGIC             WHEN response.statusCode = "200" THEN 1
# MAGIC             ELSE 0
# MAGIC           END
# MAGIC         )
# MAGIC       ) / count(lower(actionName))
# MAGIC     ),
# MAGIC     "#%"
# MAGIC   ) as `Successful %`,
# MAGIC   count(lower(actionName)) as `Total`,
# MAGIC   SUM(
# MAGIC     (
# MAGIC       CASE
# MAGIC         WHEN response.statusCode = "400" THEN 1
# MAGIC         ELSE 0
# MAGIC       END
# MAGIC     )
# MAGIC   ) as `Client Error`,
# MAGIC   SUM(
# MAGIC     (
# MAGIC       CASE
# MAGIC         WHEN response.statusCode = "403" THEN 1
# MAGIC         ELSE 0
# MAGIC       END
# MAGIC     )
# MAGIC   ) as `Access Error`,
# MAGIC   SUM(
# MAGIC     (
# MAGIC       CASE
# MAGIC         WHEN response.statusCode = "404" THEN 1
# MAGIC         ELSE 0
# MAGIC       END
# MAGIC     )
# MAGIC   ) as `Not Found Error`,
# MAGIC   SUM(
# MAGIC     (
# MAGIC       CASE
# MAGIC         WHEN response.statusCode <> "200" THEN 1
# MAGIC         ELSE 0
# MAGIC       END
# MAGIC     )
# MAGIC   ) as `Total Failed`,
# MAGIC   SUM(
# MAGIC     (
# MAGIC       CASE
# MAGIC         WHEN response.statusCode = "200" THEN 1
# MAGIC         ELSE 0
# MAGIC       END
# MAGIC     )
# MAGIC   ) as `Total Successful`
# MAGIC from
# MAGIC   unitycatalog
# MAGIC where(
# MAGIC     date >= getArgument("1_Date_Begin")
# MAGIC     AND date <= getArgument('2_Date_End')
# MAGIC   )
# MAGIC group by
# MAGIC   lower(actionName),
# MAGIC   email,
# MAGIC   requestParams.recipient_name
# MAGIC order by
# MAGIC   `Total Failed` desc

# COMMAND ----------

# MAGIC %md
# MAGIC ### Recipient Error Frequency

# COMMAND ----------

# MAGIC %sql
# MAGIC -- date table to make sure you get days without errors and a full time series
# MAGIC select
# MAGIC   requestParams.recipient_name,
# MAGIC   date,
# MAGIC   response.statusCode,
# MAGIC   count(*) as numErrors
# MAGIC from
# MAGIC   unitycatalog
# MAGIC where
# MAGIC   response.statusCode <> 200
# MAGIC   AND requestParams.recipient_name is not null
# MAGIC   AND (
# MAGIC     date >= getArgument("1_Date_Begin")
# MAGIC     AND date <= getArgument('2_Date_End')
# MAGIC   )
# MAGIC group by
# MAGIC   1,
# MAGIC   2,
# MAGIC   3
# MAGIC order by
# MAGIC   date;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Recipient Error Details

# COMMAND ----------

# MAGIC %sql
# MAGIC -- agent, time, error message, IP address, response
# MAGIC --last week or last two weeks (what you care about for failures)
# MAGIC SELECT
# MAGIC   u.date,
# MAGIC   email,
# MAGIC   requestParams.recipient_name,
# MAGIC   lower(actionName) as `Actions`,
# MAGIC   count(lower(actionName)) as `Total Failures`,
# MAGIC   response.statusCode,
# MAGIC   CASE
# MAGIC     WHEN response.statusCode = "400" THEN "Client Error"
# MAGIC     ELSE CASE
# MAGIC       WHEN response.statusCode = "403" THEN "Access Error"
# MAGIC       ELSE CASE
# MAGIC         WHEN response.statusCode = "404" THEN "Not Found Error"
# MAGIC         ELSE CASE
# MAGIC           WHEN response.statusCode = "200" THEN "Success"
# MAGIC           ELSE response.statusCode
# MAGIC         END
# MAGIC       END
# MAGIC     END
# MAGIC   END as Responses,
# MAGIC   response.errorMessage,
# MAGIC   response.result,
# MAGIC   sourceIPAddress,
# MAGIC   CASE
# MAGIC     WHEN userAgent is NULL THEN requestParams.user_agent
# MAGIC     ELSE userAgent
# MAGIC   END as `User Agent`,
# MAGIC   requestParams
# MAGIC from
# MAGIC   unitycatalog u
# MAGIC where
# MAGIC   response.statusCode <> "200"
# MAGIC   AND (
# MAGIC     date >= getArgument("1_Date_Begin")
# MAGIC     AND date <= getArgument('2_Date_End')
# MAGIC   )
# MAGIC group by
# MAGIC   email,
# MAGIC   lower(actionName),
# MAGIC   date,
# MAGIC   response.errorMessage,
# MAGIC   response.statusCode,
# MAGIC   sourceIPAddress,
# MAGIC   response.result,
# MAGIC   userAgent,
# MAGIC   requestParams
# MAGIC order by
# MAGIC   date desc

# COMMAND ----------

# MAGIC %md
# MAGIC ### Recipient IP address geolocation

# COMMAND ----------

import ipaddress
from pyspark.sql.types import StringType
from pyspark.sql.functions import col, sequence, explode, lit

@udf(returnType=StringType())
def ip_network(address:str, netmask:int) -> str:
    try:
        ipaddress.ip_address(address)
        return str(ipaddress.ip_network(f'{address}/{netmask}', strict=False).network_address)
    except:
        return 'not valid IP'

# COMMAND ----------

# group by CIDR range, (different CIDR)
# join against existing geolocation datasets to map (https://dev.maxmind.com/geoip/geolite2-free-geolocation-data)

geolocation_df = spark.sql("""
    SELECT *
      , REGEXP_EXTRACT(network, r'(.*)/' ) network_bin
      , CAST(REGEXP_EXTRACT(network, r'/(.*)' ) AS long) mask
    FROM main.default.geolite2_city_blocks_ipv4
    JOIN main.default.geolite2_city_locations
    USING(geoname_id)
""")

df = (spark.table("unitycatalog")
      .where("sourceIpAddress is not null and requestParams.recipient_name is not null")
      .withColumn("mask", explode(sequence(lit(7),lit(32))))
      .withColumn("network_bin", ip_network("sourceIpAddress", "mask"))
      .join(geolocation_df, ['network_bin', 'mask'])
      .where("city_name IS NOT null")
      .groupBy("requestParams.recipient_name", "date", "city_name", "country_name")
      .count()
      .orderBy(col("recipient_name").asc(), col("date").desc())
     )

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Recipient Query Data Frequency

# COMMAND ----------

# MAGIC %sql --query
# MAGIC -- notifications of table versions
# MAGIC -- metadata vs. query
# MAGIC select
# MAGIC   requestParams.recipient_name,
# MAGIC   date,
# MAGIC   count(*) as numQueries
# MAGIC from
# MAGIC   unitycatalog
# MAGIC where
# MAGIC   LOWER(actionName) like "deltasharingquer%"
# MAGIC   AND (
# MAGIC     date >= getArgument("1_Date_Begin")
# MAGIC     AND date <= getArgument('2_Date_End')
# MAGIC   )
# MAGIC group by
# MAGIC   1,
# MAGIC   2
# MAGIC order by
# MAGIC   1 asc,
# MAGIC   2 asc

# COMMAND ----------

# MAGIC %sql --
# MAGIC select
# MAGIC   recipient_name,
# MAGIC   date,
# MAGIC   `User Agent`,
# MAGIC   num_actions
# MAGIC from
# MAGIC   (
# MAGIC     select
# MAGIC       requestParams.recipient_name as recipient_name,
# MAGIC       date,CASE
# MAGIC         WHEN userAgent is NULL THEN requestParams.user_agent
# MAGIC         ELSE userAgent
# MAGIC       END as `User Agent`,
# MAGIC       count(*) as num_actions
# MAGIC     from
# MAGIC       unityCatalog
# MAGIC     WHERE
# MAGIC       date >= getArgument("1_Date_Begin")
# MAGIC       AND date <= getArgument('2_Date_End')
# MAGIC     group by
# MAGIC       requestParams.recipient_name,
# MAGIC       date,
# MAGIC       userAgent,
# MAGIC       requestParams
# MAGIC     order by
# MAGIC       recipient_name ASC,
# MAGIC       date DESC,
# MAGIC       num_actions DESC
# MAGIC   )
# MAGIC where
# MAGIC   num_actions > 0;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Recipient Activities

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC   recipient_name,
# MAGIC   date,
# MAGIC   actionName,
# MAGIC   num_actions
# MAGIC from
# MAGIC   (
# MAGIC     select
# MAGIC       requestParams.recipient_name as recipient_name,
# MAGIC       date,
# MAGIC       actionName,
# MAGIC       count(*) as num_actions
# MAGIC     from
# MAGIC       unityCatalog
# MAGIC     WHERE
# MAGIC       date >= getArgument("1_Date_Begin")
# MAGIC       AND date <= getArgument('2_Date_End')
# MAGIC     group by
# MAGIC       requestParams.recipient_name,
# MAGIC       date,
# MAGIC       requestParams,
# MAGIC       actionName
# MAGIC     order by
# MAGIC       recipient_name ASC,
# MAGIC       date DESC,
# MAGIC       num_actions DESC
# MAGIC   )
# MAGIC where
# MAGIC   num_actions > 0;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Recipients Top Tables

# COMMAND ----------

# DBTITLE 0,Recipients Top Tables
# MAGIC %sql --action item ask lin what "queriedtable"
# MAGIC -- hierarchy calc (english sentence), concat the three level namespace and give an explanation
# MAGIC SELECT
# MAGIC   DISTINCT --actionName,
# MAGIC   CASE
# MAGIC     WHEN CONCAT(
# MAGIC       requestParams.share,
# MAGIC       ".",
# MAGIC       requestParams.schema,
# MAGIC       ".",
# MAGIC       requestParams.name
# MAGIC     ) is null THEN CAST(
# MAGIC       from_json(response.result, 'tableName STRING').tableName AS STRING
# MAGIC     )
# MAGIC     ELSE CONCAT(
# MAGIC       requestParams.share,
# MAGIC       ".",
# MAGIC       requestParams.schema,
# MAGIC       ".",
# MAGIC       requestParams.name
# MAGIC     )
# MAGIC   END as Asset,
# MAGIC   date,
# MAGIC   MIN(date) as `First Usage Date`,
# MAGIC   MAX(date) as `Max Usage Date`,
# MAGIC   CASE
# MAGIC     WHEN response.statusCode = "200" THEN count(actionName)
# MAGIC     ELSE 0
# MAGIC   END as `Successful Actions`,
# MAGIC   CASE
# MAGIC     WHEN response.statusCode = "403" THEN count(actionName)
# MAGIC     ELSE ""
# MAGIC   END as `Failed Actions`,
# MAGIC   count(actionName) as `Total Actions`,
# MAGIC   format_number(
# MAGIC     (
# MAGIC       CASE
# MAGIC         WHEN response.statusCode = "200" THEN count(actionName)
# MAGIC         ELSE 0
# MAGIC       END / count(actionName)
# MAGIC     ),
# MAGIC     "#%"
# MAGIC   ) as `Percent Successful`
# MAGIC from
# MAGIC   unitycatalog
# MAGIC where
# MAGIC   (
# MAGIC     CASE
# MAGIC       WHEN CONCAT(
# MAGIC         requestParams.share,
# MAGIC         requestParams.schema,
# MAGIC         requestParams.name
# MAGIC       ) IS NULL THEN CAST(
# MAGIC         from_json(response.result, 'tableName STRING').tableName AS STRING
# MAGIC       )
# MAGIC       ELSE CONCAT(
# MAGIC         requestParams.share,
# MAGIC         requestParams.schema,
# MAGIC         requestParams.name
# MAGIC       )
# MAGIC     END
# MAGIC   ) is not null
# MAGIC   AND date >= getArgument("1_Date_Begin")
# MAGIC   AND date <= getArgument('2_Date_End')
# MAGIC group by
# MAGIC   requestParams,
# MAGIC   --actionName,
# MAGIC   response,
# MAGIC   date
# MAGIC order by
# MAGIC   Asset ASC,
# MAGIC   `Total Actions` desc
# MAGIC limit
# MAGIC   100

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Bytes of Shared Data Recipients are Accessing (Starting 11/05)

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC   recipient_name,
# MAGIC   tableName,
# MAGIC   date,
# MAGIC   sum(numBytes) as numBytes
# MAGIC from
# MAGIC   (
# MAGIC     select
# MAGIC       requestParams.recipient_name as recipient_name,
# MAGIC       date,
# MAGIC       --CONCAT(requestParams.share,requestParams.schema,requestParams.name) as Share_Database_Table,date,
# MAGIC       CAST(
# MAGIC         from_json(response.result, 'tableName STRING').tableName AS STRING
# MAGIC       ) as tableName,
# MAGIC       CAST(
# MAGIC         from_json(response.result, 'scannedAddFileSize STRING').scannedAddFileSize AS BIGINT
# MAGIC       ) as numBytes,
# MAGIC       CAST(
# MAGIC         from_json(response.result, 'numRecords STRING').numRecords AS BIGINT
# MAGIC       ) as numRecords,
# MAGIC       CAST(
# MAGIC         from_json(response.result, 'activeAddFiles STRING').activeAddFiles AS BIGINT
# MAGIC       ) as numFiles,
# MAGIC       actionName
# MAGIC     from
# MAGIC       unitycatalog
# MAGIC     WHERE
# MAGIC       date >= getArgument("1_Date_Begin")
# MAGIC       AND date <= getArgument('2_Date_End')
# MAGIC   )
# MAGIC WHERE
# MAGIC   numBytes is not null
# MAGIC group by
# MAGIC   recipient_name,
# MAGIC   tableName,
# MAGIC   date
# MAGIC order by
# MAGIC   numBytes desc;

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC   requestParams.recipient_name as recipient_name,
# MAGIC   date,
# MAGIC   actionName,
# MAGIC   CAST(
# MAGIC     from_json(response.result, 'metastoreId STRING').metastoreId AS STRING
# MAGIC   ) as metastoreId,
# MAGIC   CAST(
# MAGIC     from_json(response.result, 'tableName STRING').tableName AS STRING
# MAGIC   ) as tableName,
# MAGIC   CAST(
# MAGIC     from_json(response.result, 'checkpointBytes STRING').checkpointBytes AS BIGINT
# MAGIC   ) as checkpointBytes,
# MAGIC   CAST(
# MAGIC     from_json(response.result, 'maxRemoveFiles STRING').maxRemoveFiles AS BIGINT
# MAGIC   ) as maxRemoveFiles,
# MAGIC   CAST(
# MAGIC     from_json(response.result, 'path STRING').path AS STRING
# MAGIC   ) as filepath,
# MAGIC   CAST(
# MAGIC     from_json(response.result, 'scannedAddFileSize STRING').scannedAddFileSize AS BIGINT
# MAGIC   ) as numBytes,
# MAGIC   CAST(
# MAGIC     from_json(response.result, 'numRecords STRING').numRecords AS BIGINT
# MAGIC   ) as numRecords,
# MAGIC   CAST(
# MAGIC     from_json(response.result, 'activeAddFiles STRING').activeAddFiles AS BIGINT
# MAGIC   ) as numFiles,
# MAGIC   CAST(
# MAGIC     from_json(response.result, 'limitHint STRING').limitHint AS STRING
# MAGIC   ) as limitHint
# MAGIC from
# MAGIC   unitycatalog
# MAGIC where
# MAGIC   date >= getArgument("1_Date_Begin")
# MAGIC   AND date <= getArgument('2_Date_End')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Amount of Shared Data Recipients are Accessing (Starting 11/05)

# COMMAND ----------

# MAGIC %sql -- no user agent with queried table
# MAGIC select
# MAGIC   recipient_name,
# MAGIC   actionName,
# MAGIC   date,
# MAGIC   sum(numBytes) as numBytes,
# MAGIC   sum(numRecords) as numRecords,
# MAGIC   sum(numFiles) as numFiles
# MAGIC from
# MAGIC   (
# MAGIC     select
# MAGIC       requestParams.recipient_name as recipient_name,
# MAGIC       date,
# MAGIC       CAST(
# MAGIC         from_json(response.result, 'scannedAddFileSize STRING').scannedAddFileSize AS BIGINT
# MAGIC       ) as numBytes,
# MAGIC       CAST(
# MAGIC         from_json(response.result, 'numRecords STRING').numRecords AS BIGINT
# MAGIC       ) as numRecords,
# MAGIC       CAST(
# MAGIC         from_json(response.result, 'activeAddFiles STRING').activeAddFiles AS BIGINT
# MAGIC       ) as numFiles,
# MAGIC       actionName
# MAGIC     from
# MAGIC       unitycatalog
# MAGIC     WHERE
# MAGIC       date >= getArgument("1_Date_Begin")
# MAGIC       AND date <= getArgument('2_Date_End')
# MAGIC   )
# MAGIC group by
# MAGIC   recipient_name,
# MAGIC   date,
# MAGIC   actionName
# MAGIC order by
# MAGIC   date DESC,
# MAGIC   numBytes desc;
