# Databricks notebook source
import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *
import json

# edit the full table name to match the silver tables the first pipeline writes out to
services_table = "vuong_nguyen_audit.silver_services_schema"
silver_table = "vuong_nguyen_audit.silver"

# COMMAND ----------

# retrieve the list of services
def get_services():
    return [s[0] for s in spark.table(services_table).collect()]

# retrieve the schema for the service
def get_schema(service_name):
    return (spark.table(services_table)
            .filter(col("serviceName") == service_name)
            .select("schema")
            .collect()[0][0]
           )

# this function will generate a gold table for the specified service by filtering from the silver table
def generate_gold_tables(service):
    # sanitising the table name
    service_name = service.replace("-","_")  
    schema = StructType.fromJson(json.loads(get_schema(service)))            
    
    @dlt.table(
        name=service_name,
        comment=f"Gold table for {service_name}",
        table_properties={"quality":"gold"},
        partition_cols = [ 'date' ]
      )    
    def create_gold_table():      
        return (spark.readStream
                .table(silver_table)
                .filter(col("serviceName") == service)
                .withColumn("requestParams", from_json(col("flattened"), schema))
                .drop("flattened")
               )
    
# programmatically generate gold tables, for each service in the silver table
for service in get_services():
    generate_gold_tables(service)
