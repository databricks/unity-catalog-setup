<!-- BEGIN_TF_DOCS -->
Create AWS Objects for Unity Catalog Metastore
******************************************************************/

## Requirements

| Name | Version |
|------|---------|
| <a name="requirement_aws"></a> [aws](#requirement\_aws) | ~>3.68.0 |

## Providers

| Name | Version |
|------|---------|
| <a name="provider_aws"></a> [aws](#provider\_aws) | ~>3.68.0 |
| <a name="provider_random"></a> [random](#provider\_random) | n/a |

## Modules

No modules.

## Resources

| Name | Type |
|------|------|
| [aws_iam_policy.unity_metastore](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/iam_policy) | resource |
| [aws_iam_role.unity_metastore](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/iam_role) | resource |
| [aws_s3_bucket.unity_metastore](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/s3_bucket) | resource |
| [random_string.naming](https://registry.terraform.io/providers/hashicorp/random/latest/docs/resources/string) | resource |
| [aws_iam_policy_document.unity_trust_relationship](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/data-sources/iam_policy_document) | data source |

## Inputs

| Name | Description | Type | Default | Required |
|------|-------------|------|---------|:--------:|
| <a name="input_databricks_account_id"></a> [databricks\_account\_id](#input\_databricks\_account\_id) | n/a | `string` | n/a | yes |
| <a name="input_unity_metastore_bucket"></a> [unity\_metastore\_bucket](#input\_unity\_metastore\_bucket) | n/a | `string` | n/a | yes |

## Outputs

| Name | Description |
|------|-------------|
| <a name="output_unity_metastore_bucket"></a> [unity\_metastore\_bucket](#output\_unity\_metastore\_bucket) | n/a |
| <a name="output_unity_metastore_iam"></a> [unity\_metastore\_iam](#output\_unity\_metastore\_iam) | n/a |
<!-- END_TF_DOCS -->