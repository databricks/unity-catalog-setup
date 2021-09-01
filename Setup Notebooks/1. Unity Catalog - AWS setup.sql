-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## AWS Setup required by Unity Catalog
-- MAGIC 
-- MAGIC This is based on details in "Unity Catalog Setup Guide"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC You can use AWS cli to create the AWS S3 bucket, IAM roles and policies. You can also perform the steps via the AWS console UI
-- MAGIC 
-- MAGIC - Download and install AWS cli on your desktop
-- MAGIC - execute "aws configure" and enter the required AWS credentials

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Create file uc-metastore-role.json
-- MAGIC 
-- MAGIC - replace $BUCKET with your S3 bucket name

-- COMMAND ----------



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

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Create file uc-metastore-role-trust-rel.json.json
-- MAGIC 
-- MAGIC - use content below as is

-- COMMAND ----------

{

  "Version": "2012-10-17",

  "Statement": [

    {

      "Effect": "Allow",

      "Principal": {

        "AWS": "arn:aws:iam::414351767826:role/unity-catalog-prod-UCMasterRole-14S5ZJVKOTYTL"

      },

      "Action": "sts:AssumeRole"

    },

    {

      "Effect": "Allow",

      "Principal": {

        "Service": "ec2.amazonaws.com"

      },

      "Action": "sts:AssumeRole"

    }

  ]

}


-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC # Step 1 Create a new S3 bucket
-- MAGIC 
-- MAGIC Create a S3 storage bucket ($BUCKET). This S3 bucket is the default storage location for managed tables in Unity Catalog. Use a dedicated bucket for each metastore.
-- MAGIC 
-- MAGIC # Setup 2 the IAM roles and policies
-- MAGIC 
-- MAGIC Create an IAM Role in the same AWS account as the S3 bucket and grant it access to the storage bucket.  
-- MAGIC 
-- MAGIC ### Step 2.1 Create AWS iam Role
-- MAGIC 
-- MAGIC `aws iam create-role --role-name [your_prefix]-uc-metastore-role  --assume-role-policy-document file://uc-metastore-role-trust-rel.json`
-- MAGIC 
-- MAGIC ### Step 2.2 Create AWS iam policy
-- MAGIC 
-- MAGIC `aws iam create-policy --policy-name [your_prefix]-uc-metastore-policy --policy-document file://uc-metastore-role.json `
-- MAGIC 
-- MAGIC ### Step 2.3 Setup trust relationship
-- MAGIC Get the policy arn from above step and then use it in the below command
-- MAGIC 
-- MAGIC `aws iam attach-role-policy --role-name [your_prefix]-uc-metastore-role --policy-arn "arn:aws:iam::[.....]:policy/[your_prefix]-uc-metastore-policy"`
-- MAGIC 
-- MAGIC ### Step 2.4 Verify that the policies are attached to the role
-- MAGIC 
-- MAGIC `aws iam list-attached-role-policies --role-name [your_prefix]-uc-metastore-role`
