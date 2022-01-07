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
  pushd terraform/azure || exit
  terraform apply
  popd || exit
fi

if [[ $cloud_type == "2" ]] ; then
  pushd terraform/aws || exit
  terraform init
  terraform apply
  popd || exit
fi

