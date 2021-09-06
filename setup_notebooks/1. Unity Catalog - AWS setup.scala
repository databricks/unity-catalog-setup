// Databricks notebook source
// MAGIC %md
// MAGIC ## AWS Setup required by Unity Catalog
// MAGIC 
// MAGIC This is based on details in "Unity Catalog Setup Guide"

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC You can use aws-cli to create the AWS S3 bucket, IAM roles and policies. You can also perform the steps via the AWS console UI
// MAGIC 
// MAGIC - Download and install aws-cli on your desktop
// MAGIC - execute "aws configure" and enter the required AWS credentials

// COMMAND ----------

// MAGIC %md
// MAGIC ### Step 0: prepare json files for aws-cli (not required for console UI)

// COMMAND ----------

// MAGIC %md
// MAGIC #### uc-metastore-role.json
// MAGIC 
// MAGIC - replace $BUCKET with your S3 bucket name

// COMMAND ----------

{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Action": [
                "s3:GetObject",
                "s3:GetObjectVersion",
                "s3:PutObject",
                "s3:PutObjectAcl",
                "s3:DeleteObject",
                "s3:ListBucket",
                "s3:GetBucketLocation"
            ],
            "Resource": [
                "arn:aws:s3:::$BUCKET/*",
                "arn:aws:s3:::$BUCKET"
            ],
            "Effect": "Allow"
        },
      // add the following to access Databricks sample data
        {
            "Action": [
                "s3:GetObject",
                "s3:GetObjectVersion",
                "s3:ListBucket",
                "s3:GetBucketLocation"
            ],
            "Resource": [
                "arn:aws:s3:::databricks-corp-training/*",
                "arn:aws:s3:::databricks-corp-training"
            ],
            "Effect": "Allow"
        }
    ]
}

// COMMAND ----------

// MAGIC %md
// MAGIC #### uc-metastore-role-trust-rel.json
// MAGIC 
// MAGIC - Replace $DATABRICKS_ACCOUNT_ID with your Databricks account ID (not AWS)

// COMMAND ----------

{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": {
                "AWS": "arn:aws:iam::414351767826:role/unity-catalog-prod-UCMasterRole-14S5ZJVKOTYTL"
            },
            "Action": "sts:AssumeRole",
            "Condition": {
                "StringEquals": {
                    "sts:ExternalId": "$DATABRICKS_ACCOUNT_ID" // your databricks account ID, notice that it’s not aws account id
                }
            }
        }
    ]
}

// COMMAND ----------

// MAGIC %md
// MAGIC ### Step 1: Create a new S3 bucket
// MAGIC 
// MAGIC Create a S3 storage bucket (`$BUCKET`). This S3 bucket is the default storage location for managed tables in Unity Catalog. Use a dedicated bucket for each metastore.

// COMMAND ----------

// MAGIC %md
// MAGIC ### Step 2: Set up the IAM roles and policies
// MAGIC 
// MAGIC Create an IAM Role in the same AWS account as the S3 bucket and grant it access to the storage bucket. 

// COMMAND ----------

// MAGIC %md 
// MAGIC #### Step 2.1: Create AWS IAM Role with the trust relationship
// MAGIC 
// MAGIC Either via aws-cli:
// MAGIC 
// MAGIC `aws iam create-role --role-name [your_prefix]-uc-metastore-role  --assume-role-policy-document file://uc-metastore-role-trust-rel.json`
// MAGIC 
// MAGIC or via the UI
// MAGIC 
// MAGIC ![](screenshots/create_role.png)
// MAGIC 
// MAGIC ![](screenshots/set_role.png)
// MAGIC 
// MAGIC ![](screenshots/set_trust.png)

// COMMAND ----------

// MAGIC %md
// MAGIC #### Step 2.2: Create AWS IAM Policy
// MAGIC 
// MAGIC Via aws-cli
// MAGIC 
// MAGIC `aws iam create-policy --policy-name [your_prefix]-uc-metastore-policy --policy-document file://uc-metastore-role.json `
// MAGIC 
// MAGIC or via the UI
// MAGIC 
// MAGIC ![](screenshots/create_policy.png)
// MAGIC 
// MAGIC ![](screenshots/set_policy.png)

// COMMAND ----------

// MAGIC %md
// MAGIC #### Step 2.3: Attach IAM policy to the role
// MAGIC Get the policy arn from above step and then use it in the below aws-cli command
// MAGIC 
// MAGIC `aws iam attach-role-policy --role-name [your_prefix]-uc-metastore-role --policy-arn "arn:aws:iam::[.....]:policy/[your_prefix]-uc-metastore-policy"`
// MAGIC 
// MAGIC or via the UI
// MAGIC 
// MAGIC ![](screenshots/attach_policy.png)

// COMMAND ----------

// MAGIC %md
// MAGIC #### Step 2.4: Verify that the policies are attached to the role
// MAGIC 
// MAGIC `aws iam list-attached-role-policies --role-name [your_prefix]-uc-metastore-role`
