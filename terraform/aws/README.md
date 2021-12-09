<!-- BEGIN_TF_DOCS -->
Create a Unity Catalog metastore (and the AWS bucket & IAM role if required)
****************************************************************************************/

## Requirements

| Name | Version |
|------|---------|
| <a name="requirement_aws"></a> [aws](#requirement\_aws) | ~>3.68.0 |
| <a name="requirement_databricks"></a> [databricks](#requirement\_databricks) | ~>0.3.12 |

## Providers

| Name | Version |
|------|---------|
| <a name="provider_databricks.mws"></a> [databricks.mws](#provider\_databricks.mws) | ~>0.3.12 |
| <a name="provider_databricks.workspace"></a> [databricks.workspace](#provider\_databricks.workspace) | ~>0.3.12 |

## Modules

| Name | Source | Version |
|------|--------|---------|
| <a name="module_aws_metastore"></a> [aws\_metastore](#module\_aws\_metastore) | ./modules/aws_uc_resources | n/a |
| <a name="module_unity_catalog_metastore"></a> [unity\_catalog\_metastore](#module\_unity\_catalog\_metastore) | ./modules/uc_metastore | n/a |

## Resources

| Name | Type |
|------|------|
| [databricks_catalog.quickstart_catalog](https://registry.terraform.io/providers/databrickslabs/databricks/latest/docs/resources/catalog) | resource |
| [databricks_schema.quickstart_schema](https://registry.terraform.io/providers/databrickslabs/databricks/latest/docs/resources/schema) | resource |
| [databricks_user.unity_users](https://registry.terraform.io/providers/databrickslabs/databricks/latest/docs/resources/user) | resource |

## Inputs

| Name | Description | Type | Default | Required |
|------|-------------|------|---------|:--------:|
| <a name="input_aws_profile"></a> [aws\_profile](#input\_aws\_profile) | n/a | `string` | n/a | yes |
| <a name="input_aws_region"></a> [aws\_region](#input\_aws\_region) | n/a | `string` | n/a | yes |
| <a name="input_create_aws_resources"></a> [create\_aws\_resources](#input\_create\_aws\_resources) | Specify whether to create new AWS resources for Unity Catalog or not. | `bool` | n/a | yes |
| <a name="input_databricks_account_id"></a> [databricks\_account\_id](#input\_databricks\_account\_id) | Account Id that could be found in the bottom left corner of https://accounts.cloud.databricks.com/. Not your AWS account id, or Databricks workspace id | `string` | n/a | yes |
| <a name="input_databricks_account_password"></a> [databricks\_account\_password](#input\_databricks\_account\_password) | Databricks account owner credentials | `string` | n/a | yes |
| <a name="input_databricks_account_username"></a> [databricks\_account\_username](#input\_databricks\_account\_username) | Databricks account owner credentials | `string` | n/a | yes |
| <a name="input_databricks_users"></a> [databricks\_users](#input\_databricks\_users) | List of Databricks users to be added at account-level for Unity Catalog | `list[string]` | n/a | yes |
| <a name="input_databricks_workspace_ids"></a> [databricks\_workspace\_ids](#input\_databricks\_workspace\_ids) | List of Databricks workspace ids to be enabled with Unity Catalog | `list[string]` | n/a | yes |
| <a name="input_unity_metastore_bucket"></a> [unity\_metastore\_bucket](#input\_unity\_metastore\_bucket) | Name of the Unity Catalog root bucket<br>  This is the default storage location for managed tables in Unity Catalog<br>  If create\_aws\_resources = true, a random suffix will be appended to the bucket name | `string` | n/a | yes |
| <a name="input_unity_metastore_iam"></a> [unity\_metastore\_iam](#input\_unity\_metastore\_iam) | The IAM role arn for Unity Catalog, specified only if create\_aws\_resources = false<br>  The format should be arn:aws:iam::account:role/role-name-with-path<br>  The policy & trust relationship needs to follow the documentation | `string` | `""` | no |

## Outputs

No outputs.
<!-- END_TF_DOCS -->