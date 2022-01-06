#!/bin/bash
echo "Welcome to Unity Catalog Guided Setup"
echo "#######################################"

if ! command -v terraform &> /dev/null
then
    echo "terraform could not be found"
    if ! command -v brew &> /dev/null
    then
      brew install terraform
    else
      echo "brew is not installed"
      if ! command -v snap &> /dev/null
      then
        sudo snap install terraform-snap
      else
        echo "snap is not installed, please install terraform cli manually"
        exit;
      fi
    fi
fi

echo "Please select 1 for Azure, 2 for AWS".
read -r cloud_type

re='^[1-2]+$'
if ! [[ $cloud_type =~ $re ]] ; then
   echo "error: Please select 1 for Azure, 2 for AWS" >&2; exit 1
fi

if [[ $cloud_type == "1" ]] ; then
  echo "Do you want to deploy into an existing resource group? (true/false):"
  read -r azure_reuse_rg
  echo "Enter the name of your resource group:"
  read -r azure_rg
  echo "Enter the name of your azure region, i.e. US East:"
  read -r azure_location
  echo "Enter your Azure Tenant ID:"
  read -r azure_tenant_id
  echo "Enter your Azure Subscription ID:"
  read -r azure_subscription_id
  echo "Enter a prefix to use for resource creation"
  read -r azure_prefix
  echo "Enter any additional resource tags: {}"
  read -r azure_tags
  pushd terraform/azure || exit
  terraform apply -var "rg_name=$azure_rg" \
                -var "location=$azure_location" \
                -var "reuse_rg=$azure_reuse_rg" \
                -var "tenant_id=$azure_tenant_id" \
                -var "subscription_id=$azure_subscription_id" \
                -var "prefix=$azure_prefix" \
                -var "tags=$azure_tags"
  popd || exit
fi

if [[ $cloud_type == "2" ]] ; then
  echo "What is your AWS Region? i.e. us-east-1"
  read -r aws_region
  echo "Enter the AWS profile you want to use:"
  read -r aws_profile
  echo "Do you want to create AWS Resources? (true/false)"
  read -r aws_create_aws_resources
  echo "Enter the name of your Unity Metastore S3 Bucket:"
  read -r aws_unity_metastore_bucket
  echo "Enter your Unity Metastore IAM Role:"
  read -r aws_unity_metastore_iam
  echo "Enter your databricks workspace ids:"
  read -r aws_databricks_workspace_ids
  echo "Enter your databricks workspace host:"
  read -r aws_databricks_users
  echo "Enter your users to add the account level for UC in a list i.e. ['user1', 'user2']:"
  read -r aws_databricks_workspace_host
  echo "Enter the name of the Databricks account id"
  read -r aws_databricks_account_id
  echo "Enter the name of the Databricks account username"
  read -r aws_databricks_account_username
  echo "Enter the name of the Databricks account password"
  read -r aws_databricks_account_password

  pushd terraform/aws || exit
  terraform init
  terraform apply -var "aws_region=$aws_region" \
                -var "aws_profile=$aws_profile" \
                -var "create_aws_resources=$aws_create_aws_resources" \
                -var "unity_metastore_bucket=$aws_unity_metastore_bucket" \
                -var "unity_metastore_iam=$aws_unity_metastore_iam" \
                -var "databricks_workspace_host=$aws_databricks_workspace_host" \
                -var "databricks_workspace_ids=$aws_databricks_workspace_ids" \
                -var "databricks_users=$aws_databricks_users" \
                -var "databricks_account_id=$aws_databricks_account_id" \
                -var "databricks_account_username=$aws_databricks_account_username" \
                -var "databricks_account_password=$aws_databricks_account_password"
  popd || exit
fi
