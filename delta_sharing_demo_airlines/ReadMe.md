%md
This is a demo for re-use in understanding and guiding use cases for Delta Sharing using SQL.


Steps: 
1. Please first run the UC cluster setup notebook with your specific cloud (that you will be running as the data provider either Azure or AWS) and then select multi-langage single user. This will spin up an appropriate sized cluster to initally set up the demo data - this is large enough to test some good performance tests and customization of demo as needed. 

2. After the cluster has spun up please run all of the cells in the setupenv notebook to get the data prepared. This is a one time job and will not need to be-run going forward.

3. Walk through the cover page notebook

4. Run the Provider Notebook. Note in command 17 & 18 I am taking the recipient .share profile and uploading it to DBFS to demonstrate as the reciever via databricks compute in the notebook following. All you need to do is %fs head the location in cell 17/18 and you can get the details to throw in any of our developed clients so far (java, spark, pandas, Power BI)

5. Run the Reciever Notebook



If there are any questions please reach out to Tori McCunn, tori.mccunn@databricks.com and I will assist as the Partner Solutions Architect - Delta Sharing @Databricks