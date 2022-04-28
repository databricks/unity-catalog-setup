#!/bin/bash
echo "Welcome to Unity Catalog Guided Setup"
echo "#######################################"


confirm() {
  echo "Please confirm before continuing (y/n)".
  read -r confirmed
  if [[ $confirmed == 'n' ]]; then
    exit 2
  fi
}

#Setup installers
if [[ $OSTYPE == "linux-gnu"* ]]; then
      echo "This tool will ensure that required dependencies are installed. Your permissions may be elevated via sudo during this process and you may be prompted, please do not run this command as root."
      confirm;
      if command -v yum &> /dev/null
      then
         sudo yum install -y wget unzip
      fi
      if command -v apt-get &> /dev/null
      then
        sudo apt-get install wget unzip curl
        if ! command -v terraform &> /dev/null
        then
            curl -fsSL https://apt.releases.hashicorp.com/gpg | sudo apt-key add -
             sudo apt-add-repository "deb [arch=$(dpkg --print-architecture)] https://apt.releases.hashicorp.com $(lsb_release -cs) main"
             sudo apt update
             sudo apt install terraform
             terraform -v
          fi
      fi
      if ! command -v terraform &> /dev/null
      then
        sudo wget https://releases.hashicorp.com/terraform/1.1.3/terraform_1.1.3_linux_amd64.zip
        sudo unzip terraform_1.1.3_linux_amd64.zip
        sudo mv ./terraform /usr/local/bin/
      fi
elif [[ "$OSTYPE" == "darwin"* ]]; then
      echo "This tool will ensure that required dependencies are installed. Your permissions may be elevated during this process and you may be prompted to enter your credentials or to confirm, please do not run this command as root."
      confirm;
      if ! command -v brew &> /dev/null
      then
          echo "brew is not installed, installing..."
          -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
      fi
      if ! command -v terraform &> /dev/null
      then
          brew update
          brew install terraform
      fi
fi

echo "Please select 1 for Azure, 2 for AWS".
read -r cloud_type

re='^[1-2]+$'
if ! [[ $cloud_type =~ $re ]] ; then
   echo "error: Please select 1 for Azure, 2 for AWS" >&2; exit 1
fi

if [[ $cloud_type == "1" ]] ; then
  pushd terraform_files/azure || exit
  terraform init
  terraform apply
  popd || exit
fi

if [[ $cloud_type == "2" ]] ; then
  pushd terraform_files/aws || exit
  terraform init
  terraform apply
  popd || exit
fi
