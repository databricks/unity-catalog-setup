# Terraform setup for Azure UC resources

This Terraform stack creates the storage account, container and service principal required to setup Unity Catalog

## Inputs

| Name | Description | Type | Default | Required |
|------|-------------|------|---------|:--------:|
| tenant_id | Azure AD tenant ID | `string` | n/a | yes |
| subscription_id | Azure subscription ID to deploy UC resources | `string` | n/a | yes |
| rg\_name | Name of Azure resouce group where UC resources are deployed | `string` | `"unitycatalog"` | yes |
| location | Location of the resource group and the storage account. If an existing resource group is specified, the location of the existing rg is used | `string` | `"West US"` | yes |
| reuse_rg | Reuse an existing resource group | `bool` | `false` | yes |
| prefix | Prefix of all resources created. A random string will be suffixed at the end | `string` | n/a | yes |
| tags | Tags applied to all resources created | `map(string)` | n/a | yes |

## Outputs

| Name | Description |
|------|-------------|
| sp_application_id | Application ID of the service principal |
| sp_directory_id | Directory ID of the service principal |
| sp_secret | Client secret of the service principal. To view, run `terraform output sp_secret` |
| storage_path | ADLSv2 URI for the container  |
<!-- BEGIN_TF_DOCS -->
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
| <a name="provider_databricks"></a> [databricks](#provider\_databricks) | 0.4.5 |
| <a name="provider_random"></a> [random](#provider\_random) | 3.1.0 |

## Modules

No modules.

## Resources

| Name | Type |
|------|------|
| [azuread_application.unity_catalog](https://registry.terraform.io/providers/hashicorp/azuread/latest/docs/resources/application) | resource |
| [azuread_application_password.unity_catalog](https://registry.terraform.io/providers/hashicorp/azuread/latest/docs/resources/application_password) | resource |
| [azuread_service_principal.unity_catalog](https://registry.terraform.io/providers/hashicorp/azuread/latest/docs/resources/service_principal) | resource |
| [azurerm_resource_group.unity_catalog](https://registry.terraform.io/providers/hashicorp/azurerm/latest/docs/resources/resource_group) | resource |
| [azurerm_role_assignment.example](https://registry.terraform.io/providers/hashicorp/azurerm/latest/docs/resources/role_assignment) | resource |
| [azurerm_storage_account.unity_catalog](https://registry.terraform.io/providers/hashicorp/azurerm/latest/docs/resources/storage_account) | resource |
| [azurerm_storage_container.unity_catalog](https://registry.terraform.io/providers/hashicorp/azurerm/latest/docs/resources/storage_container) | resource |
| [databricks_catalog.catalog](https://registry.terraform.io/providers/databrickslabs/databricks/latest/docs/resources/catalog) | resource |
| [databricks_metastore.this](https://registry.terraform.io/providers/databrickslabs/databricks/latest/docs/resources/metastore) | resource |
| [databricks_metastore_assignment.this](https://registry.terraform.io/providers/databrickslabs/databricks/latest/docs/resources/metastore_assignment) | resource |
| [databricks_metastore_data_access.first](https://registry.terraform.io/providers/databrickslabs/databricks/latest/docs/resources/metastore_data_access) | resource |
| [databricks_schema.things](https://registry.terraform.io/providers/databrickslabs/databricks/latest/docs/resources/schema) | resource |
| [random_string.naming](https://registry.terraform.io/providers/hashicorp/random/latest/docs/resources/string) | resource |
| [azurerm_resource_group.unity_catalog](https://registry.terraform.io/providers/hashicorp/azurerm/latest/docs/data-sources/resource_group) | data source |

## Inputs

| Name | Description | Type | Default | Required |
|------|-------------|------|---------|:--------:|
| <a name="input_catalog_name"></a> [catalog\_name](#input\_catalog\_name) | Enter the name of a default catalog to create | `string` | n/a | yes |
| <a name="input_location"></a> [location](#input\_location) | Enter your location, i.e. West US or East US | `string` | n/a | yes |
| <a name="input_metastore_name"></a> [metastore\_name](#input\_metastore\_name) | Enter the name of the metastore, it can be scoped by environment or LOB i.e. dev/prod/sales/engr | `string` | n/a | yes |
| <a name="input_metastore_owner"></a> [metastore\_owner](#input\_metastore\_owner) | Enter the name of the metastore owner, this must be a valid email account or group name in Azure Active Directory | `string` | n/a | yes |
| <a name="input_prefix"></a> [prefix](#input\_prefix) | Enter a prefix to prepend to any created resources | `string` | n/a | yes |
| <a name="input_reuse_rg"></a> [reuse\_rg](#input\_reuse\_rg) | Reuse resource group, do not create a new resource group (enter true/false) | `bool` | n/a | yes |
| <a name="input_rg_name"></a> [rg\_name](#input\_rg\_name) | Enter your resource group name | `string` | n/a | yes |
| <a name="input_schema_name"></a> [schema\_name](#input\_schema\_name) | Enter the name of a default schema to create | `string` | n/a | yes |
| <a name="input_subscription_id"></a> [subscription\_id](#input\_subscription\_id) | Enter your subscription id from Azure Portal | `string` | n/a | yes |
| <a name="input_tags"></a> [tags](#input\_tags) | Enter a dictionary of tags to be added to any resources created | `map` | `{}` | no |
| <a name="input_tenant_id"></a> [tenant\_id](#input\_tenant\_id) | Enter your tenant id from Azure Portal | `string` | n/a | yes |
| <a name="input_workspace_ids"></a> [workspace\_ids](#input\_workspace\_ids) | List of Databricks workspace ids to be enabled with Unity Catalog | `list(string)` | n/a | yes |

## Outputs

| Name | Description |
|------|-------------|
| <a name="output_sp_application_id"></a> [sp\_application\_id](#output\_sp\_application\_id) | n/a |
| <a name="output_sp_directory_id"></a> [sp\_directory\_id](#output\_sp\_directory\_id) | n/a |
| <a name="output_sp_secret"></a> [sp\_secret](#output\_sp\_secret) | n/a |
| <a name="output_storage_path"></a> [storage\_path](#output\_storage\_path) | n/a |
<!-- END_TF_DOCS -->