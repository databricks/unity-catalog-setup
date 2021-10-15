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