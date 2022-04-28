output "unity_metastore_bucket" {
  value = aws_s3_bucket.unity_metastore.bucket
}

output "unity_metastore_iam" {
  value = aws_iam_role.unity_metastore.arn
}