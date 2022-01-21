# Unity Catalog Setup
This repository provides documentation, guidance, and scripting to support the automatic setup of Unity Catalog within your Databricks environment.

Either follow the Guided Setup or the Manual Setup instructions. Once complete, follow the section titled "After Running Guided or Manual Setup".

# Prerequisites
* Mac/Windows
* Azure/AWS CLI 
## Manual Setup Only
* Install terraform 1.1.2 or higher.
## Required Cloud Permissions
### Azure
You will need the following 3 roles in Azure to successfully run this script.
* Contributor (To create an ADLS Gen2 storage account)
* Application Administrator (To create a service principal)
* Cloud Application Administrator (To create a role assignment for ADLS access to the service principal)
### AWS
You will need at minimum the following rights in AWS to successfully run this script.
* s3:CreateBucket
* s3:PutBucketPolicy
* iam:CreateRole
* iam:CreatePolicy
* iam:PutRolePolicy
* iam:AttachRolePolicy

## Instructions
### Step 1 - Azure Only - Create a new workspace and setup SCIM
**You must do this first if you are deploying in Azure**

As a temporary measure in Azure, in order to have all groups and users synchronized to your Databricks account, you will first need to create a new workspace and setup SCIM for this workspace. See the instructions for setting up SCIM here (https://docs.microsoft.com/en-us/azure/databricks/administration-guide/users-groups/scim/). 

Once this is done, your groups and users from this SCIM will be synchronized to your account, and you can continue with the rest of the setup. This new workspace **must** be created after Account Level Identity is enabled on your Databricks Account.

### Step 2 - Install/Login to your Cloud Provider
For whichever cloud you plan to install for, install the appropriate Cloud CLI tool and then login with your credentials.

`az login`

or 

`aws configure`


### Step 3 - Guided Setup (if you are doing a manual install skip to Step 4)
- Run the guided setup from a Mac/Linux machine. This will attempt to install terraform for you, and then gather required values in Q&A mode and pass them to terraform.
- You will be prompted for variable input as part of this guided process
  - Review the variables needed for Azure [here](terraform/azure/README.md).
  - Review the variables needed for AWS [here](terraform/aws/README.md).
```commandline
./run.sh
```
Jump to Step 5

### Step 4 - Manual Setup
Manual setup is performed via terraform, ensure that you have terraform installed before continuing.
#### AWS
Change to the aws directory
```commandline
cd terraform/aws
```
Review the variables needed for AWS [here](terraform/aws/README.md).
Replace variable values as needed in `unity_catalog.tfvars` and run `terraform apply`

```commandline
terraform apply -var-file "unity_catalog.tfvars"
```

#### Azure
Change to the azure directory
```
cd terraform/azure
```
Review the variables needed for Azure [here](terraform/azure/README.md).
Replace variable values as needed in `unity_catalog.tfvars` and run `terraform apply`
```commandline
terraform apply -var-file "unity_catalog.tfvars"
```


### Step 5 - After Running Guided or Manual Setup
- Once the setup script finished, log into the workspace and look for the 3 level data catalog in the Data tab

    **Cluster Requirements**
    - **DBR Requirements**: You must use DBR 10.1+
    - **Security Mode Requirements**
      - Use Security Mode in the cluster creation UI for clusters with access to Unity Catalog (DBR 10.1+)
          - **User isolation** - This provides a cluster that can use SQL only, but can be shared by multiple users
          - **Single User** - This provides a cluster that supports multiple languages (SQL, python, scala, R), but one user must be nominated to use it exclusively.
- Import this repo into the Databricks workspace, and use the notebooks under `quickstart_notebooks` to get familiar with Unity Catalog journeys
- 
- Switch to the SQL persona and use the Data Explorer to browse through the 3 level data catalogs and view tables metadata & permissions, without a running endpoint

- **SQL Endpoints** - Under the ‘Advanced Settings’ select the ‘Preview Channel’ when creating a UC-enabled SQL endpoint

