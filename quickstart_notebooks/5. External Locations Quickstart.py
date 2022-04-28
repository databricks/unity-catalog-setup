# Databricks notebook source
# MAGIC %md
# MAGIC ## Create an External Table using External Location and Storage Credentials
# MAGIC 
# MAGIC Unity Catalog’s security and governance model provides excellent support for External Tables.  We will use the REST API to create a Storage Credential, and SQL to create an External Location and External Tables.

# COMMAND ----------

host = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().getOrElse(None)
token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().getOrElse(None)

# COMMAND ----------

## create storage credentials
## You will need the ARN for the IAM role that has access to your bucket with your data in order to create the Storage Credential.

import requests

aws_iam_arn = "arn:aws:iam::<account-id>:role/role"

credential_json = {
    "name": "test_cred",
    "aws_iam_role": {
        "role_arn": aws_iam_arn
    }
}


response = requests.post(
  host + '/api/2.0/unity-catalog/storage-credentials',
  headers={"Authorization": "Bearer " + token},
  json = credential_json)
if response.status_code == 200:
    print(response.json())
else:
    raise Exception(f'Error: {response.status_code} {response.reason}')

# COMMAND ----------

# MAGIC %sql
# MAGIC --- list existing storage credentials
# MAGIC SHOW STORAGE CREDENTIALS

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Now, you will create your own external location using your  storage credential. Run the following command. Note that this will create all the directories specified as location path. 
# MAGIC 
# MAGIC CREATE EXTERNAL LOCATION `<your_location_name>`
# MAGIC 
# MAGIC URL "s3://<your_location_path>" 
# MAGIC 
# MAGIC WITH ( STORAGE CREDENTIAL `test_cred` ); --Use the storage credential you created that has the ARN that will connect to the URL where the data is
# MAGIC 
# MAGIC -- Note the use of backticks around the <your_location_name> and <credential name>
# MAGIC -- Either single or double quotes can be used around <your_location_path>

# COMMAND ----------

# MAGIC %sql
# MAGIC -- grant your users ( e.g. data engineers) permission to create tables using the CREATE TABLE permission on the external location
# MAGIC -- To be able to navigate and list data in the external cloud storage, users also need READ FILES permission.
# MAGIC -- For the purpose of this quickstart, we are granting permissions to all users, i.e. account users
# MAGIC
# MAGIC -- Note the use of backticks around the <your_location_name> and <account users>
# MAGIC
# MAGIC GRANT CREATE TABLE, READ FILES 
# MAGIC ON EXTERNAL LOCATION `<your_location_name>`
# MAGIC TO `account users`;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Note: either single quotes or double-quotes can be used around <your_location_path>, but not backticks
# MAGIC LIST 's3://<your_location_path>'

# COMMAND ----------

# MAGIC %sql
# MAGIC --- create a new schema for ext tables
# MAGIC CREATE SCHEMA IF NOT EXISTS main.external

# COMMAND ----------

# MAGIC %sql
# MAGIC --- create an external table using CREATE TABLE query, pointing the location to the directory from above.
# MAGIC CREATE TABLE IF NOT EXISTS main.external.<table_name>
# MAGIC USING Delta
# MAGIC LOCATION 's3://<your_location_path>/../<your_table_dir>'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM main.external.<table_name>;
