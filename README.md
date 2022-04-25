# Unity Catalog Setup
This repository provides documentation, guidance, and scripting to support the automatic setup of Unity Catalog within your Databricks environment.

Either follow the Guided Setup or the Manual Setup instructions. Once complete, follow the section titled "After Running Guided or Manual Setup".

- [Unity Catalog Setup](#unity-catalog-setup)
- [Prerequisites](#prerequisites)
  - [Manual Setup Only](#manual-setup-only)
  - [Required Cloud Permissions](#required-cloud-permissions)
    - [Azure](#azure)
    - [AWS](#aws)
- [Instructions](#instructions)
    - [Step 1 - Azure Only - Create a new workspace and setup SCIM](#step-1---azure-only---create-a-new-workspace-and-setup-scim)
    - [Step 2 - Install/Login to your Cloud Provider](#step-2---installlogin-to-your-cloud-provider)
    - [Step 3 - Guided Setup (if you are doing a manual install skip to Step 4)](#step-3---guided-setup-if-you-are-doing-a-manual-install-skip-to-step-4)
    - [Step 4 - Manual Setup](#step-4---manual-setup)
      - [AWS](#aws-1)
      - [Azure](#azure-1)
    - [Step 5 - After Running Guided or Manual Setup](#step-5---after-running-guided-or-manual-setup)
- [Common Errors](#common-errors)
  - [User Already Exists](#user-already-exists)
  - [Cannot create catalog](#cannot-create-catalog)

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

This script will perform the following actions in Azure and Databricks:
* Create an ADLS Gen 2 Storage Account with the specified prefix
* Create a new Service Principal in your Active Directory
* Create a role assignment for this service Principal to your new Storage Account
* Create a new Unity Catalog metastore using the service principal and storage account
* Assign the UC metastore to the specified workspaces
* Create a Catalog in the Unity Catalog Metastore
* Create a Schema in the Unity Catalog Metastore
### AWS
You will need at minimum the following rights in AWS to successfully run this script.

* s3:CreateBucket
* s3:ListBucket
* s3:PutBucketPolicy
* iam:CreateRole
* iam:CreatePolicy
* iam:PutRolePolicy
* iam:AttachRolePolicy
* iam:UpdateAssumeRolePolicy
* iam:ListRoles
* iam:ListPolicies
* iam:ListRolePolicies
* iam:ListAttachedRolePolicies

This script will perform the following actions in AWS and Databricks:
* Create an S3 Bucket with the specified prefix
* Create a new IAM Policy to access this bucket
* Create a new IAM Role and assign the newly created Policy to the Role
* Add a trust relationship for Unity Catalog master role to assume this role
* Create a new Unity Catalog metastore using the IAM Role and Bucket
* Assign the UC metastore to the specified workspaces
* Create Databricks admins & users at account level, to use Unity Catalog
* Create a Catalog in the Unity Catalog Metastore
* Create a Schema in the Unity Catalog Metastore

# Instructions
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
  - Review the variables needed for Azure [here](terraform_files/azure/README.md).
  - Review the variables needed for AWS [here](terraform_files/aws/README.md).
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
Review the variables needed for AWS [here](terraform_files/aws/README.md).
Make a copy of `unity_catalog.tfvars`, rename it to `secrets.tfvars`, fill in the variable values as needed and run `terraform apply`

`secrets.tfvars` is already included in the `.gitignore` file to prevent an accidental push of credentials.

```commandline
terraform apply -var-file "secrets.tfvars"
```

#### Azure
Change to the azure directory
```
cd terraform/azure
```
Review the variables needed for Azure [here](terraform_files/azure/README.md).
Make a copy of `unity_catalog.tfvars`, rename it to `secrets.tfvars`, fill in the variable values as needed and run `terraform apply`

`secrets.tfvars` is already included in the `.gitignore` file to prevent an accidental push of credentials.

```commandline
terraform apply -var-file "secrets.tfvars"
```


### Step 5 - After Running Guided or Manual Setup
- Once the setup script finished, log into the workspace and look for the 3 level data catalog in the Data tab
- Create a new cluster to use Unity Catalog
  - **Cluster Requirements**
      - **DBR Requirements**: You must use DBR 10.1+
      - **Security Mode Requirements**
        - Use Security Mode in the cluster creation UI for clusters with access to Unity Catalog (DBR 10.1+)
            - **User isolation** - This provides a cluster that can use SQL only, but can be shared by multiple users
            - **Single User** - This provides a cluster that supports multiple languages (SQL, python, scala, R), but one user must be nominated to use it exclusively.
- Import this repo into the Databricks workspace, and use the notebooks under `quickstart_notebooks` to get familiar with Unity Catalog journeys
- Switch to the SQL persona and use the Data Explorer to browse through the 3 level data catalogs and view tables metadata & permissions, without a running endpoint

- **SQL Endpoints** - SQL Endpoints now support Unity Catalog by default

# Common Errors
## User Already Exists
- This happens if the user already exists in a different E2 Databricks account. Remove those users from the list of users/admins and rerun `terraform apply`. Check with your Databricks account on how to remediate for the users with issues

## Cannot create catalog
- This happens if the `databricks_workspace_host` is not in the `databricks_workspace_ids` list. Due to the API design, metastore resources (catalogs & schemas) will be created under the metastore currently assigned to `databricks_workspace_host`