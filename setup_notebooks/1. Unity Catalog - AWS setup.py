# Databricks notebook source
# MAGIC %md
# MAGIC ## AWS Setup required by Unity Catalog
# MAGIC 
# MAGIC This is based on details in "Unity Catalog Setup Guide"

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC You can use aws-cli to create the AWS S3 bucket, IAM roles and policies:
# MAGIC 
# MAGIC - Download and install aws-cli on your desktop
# MAGIC - execute "aws configure" and enter the required AWS credentials
# MAGIC 
# MAGIC You can also perform the steps via the AWS console UI

# COMMAND ----------

# helper function to display images in the repos
def display_img(path):
  import matplotlib.pyplot as plt
  import matplotlib as mpl
  mpl.rcParams['figure.dpi']= 150
  plt.figure(figsize=(10,10))
  img = plt.imread(path)
  imgplot = plt.imshow(img, interpolation='none')
  imgplot.axes.get_xaxis().set_visible(False)
  imgplot.axes.get_yaxis().set_visible(False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 0: prepare json files for aws-cli (not required for console UI)

# COMMAND ----------

# MAGIC %md
# MAGIC #### uc-metastore-role.json
# MAGIC 
# MAGIC - replace $BUCKET with your S3 bucket name

# COMMAND ----------

# MAGIC %md
# MAGIC ```
# MAGIC {
# MAGIC     "Version": "2012-10-17",
# MAGIC     "Statement": [
# MAGIC         {
# MAGIC             "Action": [
# MAGIC                 "s3:GetObject",
# MAGIC                 "s3:GetObjectVersion",
# MAGIC                 "s3:PutObject",
# MAGIC                 "s3:PutObjectAcl",
# MAGIC                 "s3:DeleteObject",
# MAGIC                 "s3:ListBucket",
# MAGIC                 "s3:GetBucketLocation"
# MAGIC             ],
# MAGIC             "Resource": [
# MAGIC                 "arn:aws:s3:::$BUCKET/*",
# MAGIC                 "arn:aws:s3:::$BUCKET"
# MAGIC             ],
# MAGIC             "Effect": "Allow"
# MAGIC         },
# MAGIC       // add the following to access Databricks sample data
# MAGIC         {
# MAGIC             "Action": [
# MAGIC                 "s3:GetObject",
# MAGIC                 "s3:GetObjectVersion",
# MAGIC                 "s3:ListBucket",
# MAGIC                 "s3:GetBucketLocation"
# MAGIC             ],
# MAGIC             "Resource": [
# MAGIC                 "arn:aws:s3:::databricks-corp-training/*",
# MAGIC                 "arn:aws:s3:::databricks-corp-training"
# MAGIC             ],
# MAGIC             "Effect": "Allow"
# MAGIC         }
# MAGIC     ]
# MAGIC }
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC #### uc-metastore-role-trust-rel.json
# MAGIC 
# MAGIC - Replace $DATABRICKS_ACCOUNT_ID with your Databricks account ID (not AWS)

# COMMAND ----------

# MAGIC %md
# MAGIC ```
# MAGIC {
# MAGIC     "Version": "2012-10-17",
# MAGIC     "Statement": [
# MAGIC         {
# MAGIC             "Effect": "Allow",
# MAGIC             "Principal": {
# MAGIC                 "AWS": "arn:aws:iam::414351767826:role/unity-catalog-prod-UCMasterRole-14S5ZJVKOTYTL"
# MAGIC             },
# MAGIC             "Action": "sts:AssumeRole",
# MAGIC             "Condition": {
# MAGIC                 "StringEquals": {
# MAGIC                     "sts:ExternalId": "$DATABRICKS_ACCOUNT_ID" // your databricks account ID, notice that it’s not aws account id
# MAGIC                 }
# MAGIC             }
# MAGIC         }
# MAGIC     ]
# MAGIC }
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1: Create a new S3 bucket
# MAGIC 
# MAGIC Create a S3 storage bucket (`$BUCKET`). This S3 bucket is the default storage location for managed tables in Unity Catalog. Use a dedicated bucket for each metastore.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: Set up the IAM roles and policies
# MAGIC 
# MAGIC Create an IAM Role in the same AWS account as the S3 bucket and grant it access to the storage bucket. 

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Step 2.1: Create AWS IAM Role with the trust relationship
# MAGIC 
# MAGIC Either via aws-cli:
# MAGIC 
# MAGIC `aws iam create-role --role-name [your_prefix]-uc-metastore-role  --assume-role-policy-document file://uc-metastore-role-trust-rel.json`
# MAGIC 
# MAGIC or via the UI

# COMMAND ----------

display_img('screenshots/create_role.png')
display_img('screenshots/set_role.png')
display_img('screenshots/set_trust.png')

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2.2: Create AWS IAM Policy
# MAGIC 
# MAGIC Via aws-cli
# MAGIC 
# MAGIC `aws iam create-policy --policy-name [your_prefix]-uc-metastore-policy --policy-document file://uc-metastore-role.json `
# MAGIC 
# MAGIC or via the UI

# COMMAND ----------

display_img('screenshots/create_policy.png')
display_img('screenshots/set_policy.png')

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2.3: Attach IAM policy to the role
# MAGIC Get the policy arn from above step and then use it in the below aws-cli command
# MAGIC 
# MAGIC `aws iam attach-role-policy --role-name [your_prefix]-uc-metastore-role --policy-arn "arn:aws:iam::[.....]:policy/[your_prefix]-uc-metastore-policy"`
# MAGIC 
# MAGIC or via the UI

# COMMAND ----------

display_img('screenshots/set_trust.png')

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2.4: Verify that the policies are attached to the role
# MAGIC 
# MAGIC `aws iam list-attached-role-policies --role-name [your_prefix]-uc-metastore-role`
