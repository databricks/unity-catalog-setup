## This is a demo for re-use in understanding and guiding use cases for Delta Sharing using SQL.


### Steps: 
1. Create a single-user cluster, (DBR 10.1+, select "Single User" in the security mode dropdown). 

2. Once the cluster is up and running,  run all cells in the setupenv notebook to get the data prepared. This is a one time job and will not need to be-run going forward.

3. Walk through the cover page notebook

4. Run the Provider Notebook. Note in command 17 & 18 I am taking the recipient `.share` profile and uploading it to Databricks File System to demonstrate as the recieving end of the share via delta sharing on databricks compute. All you need to do is run `%fs head <location>` cell 17/18 and you can get the details of the share profile to then plug into any of our developed clients thus far. List inclusions: (Java, Spark, Pandas, Power BI)

5. Run the Receiver Notebook



If there are any questions please reach out to Tori McCunn (tori.mccunn@databricks.com) and I will assist as the Partner Solutions Architect - Delta Sharing @Databricks