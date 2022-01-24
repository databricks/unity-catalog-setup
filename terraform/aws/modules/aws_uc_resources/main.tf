/*****************************************************************
* Create AWS Objects for Unity Catalog Metastore
******************************************************************/
resource "aws_s3_bucket" "unity_metastore" {
  bucket = "${var.unity_metastore_bucket}-${local.suffix}"
  acl    = "private"

  # this will purge data in the bucket before destroying it
  force_destroy = true

  tags = {
    Name = "Databricks Unity Catalog root bucket"
  }
}

resource "aws_iam_policy" "unity_metastore" {
  # Terraform's "jsonencode" function converts a
  # Terraform expression's result to valid JSON syntax.
  policy = jsonencode({
    Version = "2012-10-17"
    Id      = "databricks-unity-metastore"
    Statement = [
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
          aws_s3_bucket.unity_metastore.arn,
          "${aws_s3_bucket.unity_metastore.arn}/*"
        ],
        "Effect": "Allow"
      }
    ]
  })

  tags = {
    Name = "Databricks Unity Catalog IAM policy"
  }  
}

resource "aws_iam_policy" "sample_data" {
  # Terraform's "jsonencode" function converts a
  # Terraform expression's result to valid JSON syntax.
  policy = jsonencode({
    Version = "2012-10-17"
    Id      = "databricks-sample-data"
    Statement = [
      {
        "Action": [
          "s3:GetObject",
          "s3:GetObjectVersion",
          "s3:ListBucket",
          "s3:GetBucketLocation"
        ],
        "Resource": [
          "arn:aws:s3:::databricks-datasets-oregon/*",
          "arn:aws:s3:::databricks-datasets-oregon"

        ],
        "Effect": "Allow"
      }
    ]
  })
  tags = {
    Name = "Databricks Unity Catalog sample data policy"
  }
}


data "aws_iam_policy_document" "unity_trust_relationship" {
  statement {
    effect = "Allow"
    principals {
      type        = "AWS"
      identifiers = ["arn:aws:iam::414351767826:role/unity-catalog-prod-UCMasterRole-14S5ZJVKOTYTL"]
    }
    actions = ["sts:AssumeRole"]

    condition {
      test = "StringEquals"
      values = [var.databricks_account_id]
      variable = "sts:ExternalId"
    }
  }
}

resource "aws_iam_role" "unity_metastore" {
  name = "databricks_unity_catalog"
  description = "allows access to s3 bucket for the unity metastore root and creates trust relationship to the Databricks Unity Catalog Service Account"
  assume_role_policy  = data.aws_iam_policy_document.unity_trust_relationship.json
  managed_policy_arns = [aws_iam_policy.unity_metastore.arn, aws_iam_policy.sample_data.arn]

  tags = {
    Name = "Databricks Unity Catalog IAM role"
  }   
}
