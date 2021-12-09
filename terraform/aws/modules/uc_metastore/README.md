<!-- BEGIN_TF_DOCS -->
## Requirements

| Name | Version |
|------|---------|
| <a name="requirement_databricks"></a> [databricks](#requirement\_databricks) | ~>0.4.0 |

## Providers

| Name | Version |
|------|---------|
| <a name="provider_databricks"></a> [databricks](#provider\_databricks) | ~>0.4.0 |

## Modules

No modules.

## Resources

| Name | Type |
|------|------|

## Inputs

| Name | Description | Type | Default | Required |
|------|-------------|------|---------|:--------:|
| <a name="input_databricks_workspace_ids"></a> [databricks\_workspace\_ids](#input\_databricks\_workspace\_ids) | List of Databricks workspace ids to be enabled with Unity Catalog | `list(string)` | n/a | yes |
| <a name="input_unity_metastore_bucket"></a> [unity\_metastore\_bucket](#input\_unity\_metastore\_bucket) | UC metastore bucket | `string` | n/a | yes |
| <a name="input_unity_metastore_iam"></a> [unity\_metastore\_iam](#input\_unity\_metastore\_iam) | UC metastore IAM role | `string` | n/a | yes |

## Outputs

No outputs.
<!-- END_TF_DOCS -->