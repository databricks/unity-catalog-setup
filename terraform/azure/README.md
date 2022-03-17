# Terraform setup for Azure UC resources

<!-- BEGIN_TF_DOCS -->
Create a Unity Catalog metastore with its default root bucket & service principal
Create securables inside the newly created metastore: catalog, schema, external location & credentials
Create a UC-enabled cluster
******************************************************************************************************************/

## Requirements

| Name | Version |
|------|---------|
| <a name="requirement_azuread"></a> [azuread](#requirement\_azuread) | ~>2.15.0 |
| <a name="requirement_azurerm"></a> [azurerm](#requirement\_azurerm) | ~>2.92.0 |
| <a name="requirement_databricks"></a> [databricks](#requirement\_databricks) | ~>0.4.0 |

## Providers

| Name | Version |
|------|---------|
| <a name="provider_azuread"></a> [azuread](#provider\_azuread) | 2.15.0 |
| <a name="provider_azurerm"></a> [azurerm](#provider\_azurerm) | 2.92.0 |
| <a name="provider_databricks"></a> [databricks](#provider\_databricks) | 0.4.7 |
| <a name="provider_random"></a> [random](#provider\_random) | 3.1.0 |

## Modules

No modules.

## Resources

| Name | Type |
|------|------|
| [azuread_application.ext_cred](https://registry.terraform.io/providers/hashicorp/azuread/latest/docs/resources/application) | resource |
| [azuread_application.unity_catalog](https://registry.terraform.io/providers/hashicorp/azuread/latest/docs/resources/application) | resource |
| [azuread_application_password.ext_cred](https://registry.terraform.io/providers/hashicorp/azuread/latest/docs/resources/application_password) | resource |
| [azuread_application_password.unity_catalog](https://registry.terraform.io/providers/hashicorp/azuread/latest/docs/resources/application_password) | resource |
| [azuread_service_principal.ext_cred](https://registry.terraform.io/providers/hashicorp/azuread/latest/docs/resources/service_principal) | resource |
| [azuread_service_principal.unity_catalog](https://registry.terraform.io/providers/hashicorp/azuread/latest/docs/resources/service_principal) | resource |
| [azurerm_resource_group.unity_catalog](https://registry.terraform.io/providers/hashicorp/azurerm/latest/docs/resources/resource_group) | resource |
| [azurerm_role_assignment.example](https://registry.terraform.io/providers/hashicorp/azurerm/latest/docs/resources/role_assignment) | resource |
| [azurerm_role_assignment.ext_storage](https://registry.terraform.io/providers/hashicorp/azurerm/latest/docs/resources/role_assignment) | resource |
| [azurerm_storage_account.ext_storage](https://registry.terraform.io/providers/hashicorp/azurerm/latest/docs/resources/storage_account) | resource |
| [azurerm_storage_account.unity_catalog](https://registry.terraform.io/providers/hashicorp/azurerm/latest/docs/resources/storage_account) | resource |
| [azurerm_storage_container.ext_storage](https://registry.terraform.io/providers/hashicorp/azurerm/latest/docs/resources/storage_container) | resource |
| [azurerm_storage_container.unity_catalog](https://registry.terraform.io/providers/hashicorp/azurerm/latest/docs/resources/storage_container) | resource |
| [databricks_catalog.catalog](https://registry.terraform.io/providers/databrickslabs/databricks/latest/docs/resources/catalog) | resource |
| [databricks_cluster.unity_sql](https://registry.terraform.io/providers/databrickslabs/databricks/latest/docs/resources/cluster) | resource |
| [databricks_external_location.some](https://registry.terraform.io/providers/databrickslabs/databricks/latest/docs/resources/external_location) | resource |
| [databricks_metastore.this](https://registry.terraform.io/providers/databrickslabs/databricks/latest/docs/resources/metastore) | resource |
| [databricks_metastore_assignment.this](https://registry.terraform.io/providers/databrickslabs/databricks/latest/docs/resources/metastore_assignment) | resource |
| [databricks_metastore_data_access.first](https://registry.terraform.io/providers/databrickslabs/databricks/latest/docs/resources/metastore_data_access) | resource |
| [databricks_schema.things](https://registry.terraform.io/providers/databrickslabs/databricks/latest/docs/resources/schema) | resource |
| [databricks_storage_credential.external](https://registry.terraform.io/providers/databrickslabs/databricks/latest/docs/resources/storage_credential) | resource |
| [random_string.naming](https://registry.terraform.io/providers/hashicorp/random/latest/docs/resources/string) | resource |
| [azurerm_resource_group.unity_catalog](https://registry.terraform.io/providers/hashicorp/azurerm/latest/docs/data-sources/resource_group) | data source |
| [databricks_node_type.smallest](https://registry.terraform.io/providers/databrickslabs/databricks/latest/docs/data-sources/node_type) | data source |
| [databricks_spark_version.latest](https://registry.terraform.io/providers/databrickslabs/databricks/latest/docs/data-sources/spark_version) | data source |

## Inputs

| Name | Description | Type | Default | Required |
|------|-------------|------|---------|:--------:|
| <a name="input_catalog_name"></a> [catalog\_name](#input\_catalog\_name) | Enter the name of a default catalog to create | `string` | n/a | yes |
| <a name="input_databricks_workspace_host"></a> [databricks\_workspace\_host](#input\_databricks\_workspace\_host) | Databricks workspace url | `string` | n/a | yes |
| <a name="input_location"></a> [location](#input\_location) | Enter your location, i.e. West US or East US | `string` | n/a | yes |
| <a name="input_metastore_name"></a> [metastore\_name](#input\_metastore\_name) | Enter the name of the metastore, it can be scoped by environment or LOB i.e. dev/prod/sales/engr | `string` | n/a | yes |
| <a name="input_metastore_owner"></a> [metastore\_owner](#input\_metastore\_owner) | Enter the name of the metastore owner, this must be a valid email account or group name in Azure Active Directory | `string` | n/a | yes |
| <a name="input_prefix"></a> [prefix](#input\_prefix) | Enter a prefix to prepend to any created resources | `string` | n/a | yes |
| <a name="input_reuse_rg"></a> [reuse\_rg](#input\_reuse\_rg) | Reuse resource group, do not create a new resource group (enter true/false) | `bool` | n/a | yes |
| <a name="input_rg_name"></a> [rg\_name](#input\_rg\_name) | Enter your resource group name | `string` | n/a | yes |
| <a name="input_schema_name"></a> [schema\_name](#input\_schema\_name) | Enter the name of a default schema to create | `string` | n/a | yes |
| <a name="input_subscription_id"></a> [subscription\_id](#input\_subscription\_id) | Enter your subscription id from Azure Portal | `string` | n/a | yes |
| <a name="input_tags"></a> [tags](#input\_tags) | Enter a dictionary of tags to be added to any resources created | `map(any)` | `{}` | no |
| <a name="input_tenant_id"></a> [tenant\_id](#input\_tenant\_id) | Enter your tenant id from Azure Portal | `string` | n/a | yes |
| <a name="input_workspace_ids"></a> [workspace\_ids](#input\_workspace\_ids) | List of Databricks workspace ids to be enabled with Unity Catalog<br>  Enter with square brackets and double quotes<br>  e.g. ["111111111", "222222222"] | `list(string)` | n/a | yes |

## Outputs

| Name | Description |
|------|-------------|
| <a name="output_sp_application_id"></a> [sp\_application\_id](#output\_sp\_application\_id) | n/a |
| <a name="output_sp_directory_id"></a> [sp\_directory\_id](#output\_sp\_directory\_id) | n/a |
| <a name="output_sp_secret"></a> [sp\_secret](#output\_sp\_secret) | n/a |
| <a name="output_storage_path"></a> [storage\_path](#output\_storage\_path) | n/a |
<!-- END_TF_DOCS -->