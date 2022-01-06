# Unity Catalog Setup
This repository provides documentation, guidance, and scripting to support the automatic setup of Unity Catalog within your Databricks environment's.

# Run the Guided Setup
Run the guided setup from a Mac/Linux machine. This will gather required values in Q/A and pass them to terraform.
```commandline
./run.sh
```

# Terraform AWS Instructions
Change to the aws directory
```commandline
cd terraform/aws
```

Replace variable values as needed and run terraform

```commandline
terraform apply -var 'aws_region=us-east-1'
                -var 'aws_profile=aws-field-eng_databricks-power-user'
                -var 'create_aws_resources=true'
                -var 'unity_metastore_bucket=zp_uc_testing'
                -var 'unity_metastore_iam=arn:aws:iam::414351767826:role/unity-catalog-prod-UCMasterRole-14S5ZJVKOTYTL'
                -var 'databricks_workspace_ids='
                -var 'databricks_users='
                -var 'databricks_account_id='
                -var 'databricks_workspace_host='
                -var 'databricks_account_username='
                -var 'databricks_account_password='
```

# Terraform Azure Instructions
Change to the azure directory
```
cd terraform/azure
```

Replace variable values as needed and run terraform
```commandline
terraform apply -var 'rg_name=unity-catalog-testing' 
                -var 'location=US East'
                -var 'reuse_rg=true' 
                -var 'tenant_id=value' 
                -var 'subscription_id=value' 
                -var 'prefix=zp' 
                -var tags='{"donotdelete":true}'
```