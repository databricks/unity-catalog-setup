# databricks-users-federation
Provision users Databricks from workspace to account-level using Terraform

## Instruction
- Need to configure 2 databricks providers, one for the workspace & one for the account
- Example variables declaration in `example.tfvars`
- Specify the list of workspace groups to be imported at account-level
- Run `terraform plan --var-file=example.tfvars` to see what will be added to Databricks
- Run `terraform apply --var-file=example.tfvars` to start the provisioning

## Issue
- If user already exists in another E2 account, Databricks will return an error "User already exists in another account"
