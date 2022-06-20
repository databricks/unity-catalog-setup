## DLT pipelines for audit log processing

Currently, Databricks delivers audit logs for all enabled workspaces as per delivery SLA in JSON format to a customer-owned S3 bucket on AWS, or to a specified Event Hub on Azure. These audit logs contain events for specific actions related to primary resources like clusters, jobs, and the workspace. To simplify delivery and further analysis by the customers, Databricks logs each event for every action as a separate record and stores all the relevant parameters into a sparse StructType called requestParams.

In order to make this information more accessible, we recommend an ETL process based on [Delta Live Tables](https://databricks.com/product/delta-live-tables)

Our ETL process requires 2 DLT pipelines:
  
- The first pipeline (`01-raw-bronze-silver` on AWS and `01-raw-bronze-silver-event-hub` on Azure) does the following:
  - On AWS, stream from the raw JSON files that Databricks delivers using Autoloader to a bronze Delta Lake table. This creates a durable copy of the raw data that allows us to replay our ETL, should we find any issues in downstream tables.
  - On Azure, stream from the Event Hub that Databricks delivers audit logs to. This creates a durable copy of the raw data that allows us to replay our ETL, should we find any issues in downstream tables.
  - Stream from a bronze Delta Lake table to a silver Delta Lake table such that it takes the sparse requestParams StructType and strips out all empty keys for every record, along with performing some other basic transformations like parsing email address from a nested field and parsing UNIX epoch to UTC timestamp.
- The second pipeline (`02-silver-fan-gold`) then streams to individual gold Delta Lake tables for each Databricks service tracked in the audit logs. As the list of service is dynamically inferred from the data in the silver table, we need to create a separate pipeline due to current limitation

The 2 pipelines are then linked together in a multi-task job, to be executed on a regular schedule