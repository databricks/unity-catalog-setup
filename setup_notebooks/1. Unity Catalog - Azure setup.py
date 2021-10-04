# Databricks notebook source
# MAGIC %md
# MAGIC ## Azure Setup required by Unity Catalog
# MAGIC 
# MAGIC This is based on details in "Unity Catalog Setup Guide"

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC You can use Azure CLI to create the Azure Storage account, container and service principal:
# MAGIC 
# MAGIC - Download and install az-cli on your desktop
# MAGIC - execute `az login` and login through the portal
# MAGIC - If your account is associated with more than one subscriptions, then set the active subscription using `az account set --subscription <subscription-id>`
# MAGIC 
# MAGIC You can also perform the steps via the Azure portal UI

# COMMAND ----------

# helper function to display images in the repos
def display_img(path):
  import matplotlib.pyplot as plt
  import matplotlib as mpl
  mpl.rcParams['figure.dpi']= 150
  plt.figure(figsize=(10,10))
  img = plt.imread(path)
  imgplot = plt.imshow(img, interpolation='none')
  imgplot.axes.get_xaxis().set_visible(False)
  imgplot.axes.get_yaxis().set_visible(False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1: Create a new storage account and container
# MAGIC 
# MAGIC Create an Azure storage account (`$STORAGE_ACCOUNT`) with hierarchical namespace. Via az-cli: `az storage account create -n $STORAGE_ACCOUNT -g $RESOURCE_GROUP --kind StorageV2 --hns`
# MAGIC 
# MAGIC Create a container (`$CONTAINER`) inside this storage account. This container is the default storage location for managed tables in Unity Catalog. Use a dedicated container/storage account for each metastore. Via az-cli: `az storage fs create -n $CONTAINER --account-name $STORAGE_ACCOUNT --auth-mode login`

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: Set up the service principal
# MAGIC 
# MAGIC Create a service principal in the same Azure tenant and grant it access to the storage container. 

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Step 2.1: Create Azure Service principal
# MAGIC 
# MAGIC Via the cli:
# MAGIC 
# MAGIC `az ad app create --display-name $SP_NAME`
# MAGIC 
# MAGIC or via the UI [here](https://docs.microsoft.com/en-us/azure/databricks/dev-tools/api/latest/aad/service-prin-aad-token#--provision-a-service-principal-in-azure-portal)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2.2: Generate client secret for the Azure service principal
# MAGIC Via the cli:
# MAGIC `az ad app credential reset --id $SP_ID --append`

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2.3: Grant SP contributor blob contributor permission
# MAGIC 
# MAGIC Via cli
# MAGIC 
# MAGIC `az role assignment create --assignee $SP_ID \
# MAGIC --role "Storage Blob Data Contributor" \
# MAGIC --scope "/subscriptions/$SUBSCRIPTION_ID/resourceGroups/$RESOURCE_GROUP/providers/Microsoft.Storage/storageAccounts/$STORAGE_ACCOUNT/blobServices/default/containers/$CONTAINER"`
# MAGIC 
# MAGIC or via the UI [here](https://docs.microsoft.com/en-us/azure/databricks/data/data-sources/azure/adls-gen2/azure-datalake-gen2-sp-access#assign-roles)
