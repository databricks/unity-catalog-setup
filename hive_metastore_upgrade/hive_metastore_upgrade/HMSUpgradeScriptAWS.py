# Databricks notebook source
# MAGIC %md The goal of this notebook is to facilitate upgrading hive metastore tables to Unity Catalog. The logic first creates a dataframe that contains the schema name, table name, and location for each HMS table. The logic will then leverage this information to create credentials for each bucket (this part requires some input from the customer to manually indicate the arn for the iam role that has read/write access to the underlying bucket). From there it will create exernal locations at the highest order (root) directory of the bucket with the relevant credential attached. Last but not least the logic will iterate over all of the different tables and execute the relevant CTA operations to upgrade these tables to the Unity Catalog service. For external tables no data copy is needed as the Unity external table will just point to the same s3 directory path as the Hive external table-- however, for dbfs tables data cloning/copy is required.

# COMMAND ----------

# MAGIC %pip install uri

# COMMAND ----------

# Helper functions to extract relevant metadata from HMS tables

def toScala(x):
  return spark.sparkContext._jvm.scala.collection.JavaConverters.asScalaBufferConverter(x).asScala().toSeq()
def toJava(x):
  return spark.sparkContext._jvm.scala.collection.JavaConverters.seqAsJavaListConverter(x).asJava()

def getDatabaseNames():
  return toJava(spark._jsparkSession.sharedState().externalCatalog().unwrapped().client().listDatabases("*"))

def getTables(db):
  # Don't read each delta log because we don't care about schemas
  spark.conf.set("spark.databricks.env", "thrift-server")
  spark.conf.set("spark.databricks.delta.catalog.useCatalogSchemaInThrift", "true")
  tableNames = toJava(spark._jsparkSession.sharedState().externalCatalog().unwrapped().client().listTables(db))
  identifiers = [spark.sparkContext._jvm.org.apache.spark.sql.catalyst.TableIdentifier(tn, spark.sparkContext._jvm.scala.Some(db)) for tn in tableNames]
  rawTables = toJava(spark._jsparkSession.sessionState().catalog().getTablesByName(toScala(identifiers)))
  return [x for x in rawTables if x.storage().locationUri().isDefined()]

dbfsFs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jvm.java.net.URI("dbfs:/"), spark._jsparkSession.sessionState().newHadoopConf())
def resolveDbfsPath(path):
  global dbfsFs
  tuple = dbfsFs.delegate().resolve(spark._jvm.org.apache.hadoop.fs.Path(path))
  fs = tuple._1()
  path = tuple._2()
  return fs.resolvePath(path).toString()

# COMMAND ----------

def resolveLocation(location):
  from uri import URI
  result = []
  u = URI(location)
  if u.scheme == 'abfs' or u.scheme == 'abfss':
    return location
  elif u.scheme == 'adl' or u.scheme == 'adls':
    return location
  elif u.scheme == 's3' or u.scheme == 's3a':
    return location
  elif u.scheme == 'dbfs':
    return resolveDbfsPath(location)

def do_bootstrap(databases):
  namearray = []
  locationarray = []
  for db in databases:
    for table in getTables(db):
      db = table.database()
      name = table.qualifiedName()
      location = resolveLocation(table.location().toString())
      namearray.append(name)
      locationarray.append(location)
  maindict = {'name': namearray, 'location': locationarray}
  return maindict

# COMMAND ----------

dbnames = getDatabaseNames()
print(dbnames)

# COMMAND ----------

import pandas as pd
dict = do_bootstrap(dbnames)
maindf = pd.DataFrame(dict)
display(maindf)

# COMMAND ----------

#This cell basically allows us to parse the locations to infer how many distinct s3 buckets have been written to in Hive. We will use this information in the subsequent cell.
maindf['findindex'] = maindf.location.str.find("s3")
maindfs3 = maindf[maindf.findindex > -1]
maindfdbfs = maindf[maindf.findindex == -1]
sparkdf = spark.createDataFrame(maindfs3)

sparkdf.createOrReplaceTempView("s3bucketview")
s3buckettable = sql('select name, location, substring(location, locate("//", location) + 2, locate("/", location, 6) - 6) as bucketroot from s3bucketview')
s3bucketpd = s3buckettable.toPandas()
display(s3bucketpd)

# COMMAND ----------

# This is the main cell in the logic that involves manual intervention. Essentially bucketroot returns the distinct s3 buckets that have been written to in HMS. The user would then create an arrary of strings containing the arn of the corresponding iam roles that have access to these buckets. We then create a df that joins these two columns and feed the df into the below functions

bucketpath = s3bucketpd.bucketroot.unique()
bucketpathiamrole = ["insertarnhere"]
datadict = {'bucketroot':bucketpath, 'iamrole': bucketpathiamrole}
bucketpathdf = pd.DataFrame(datadict)
display(bucketpathdf)

# COMMAND ----------

# This function will call the credentials api to create new storage credentials

import json
import requests
import pandas as pd

from requests.auth import HTTPBasicAuth

host = 'inserthosthere'
un = 'insertunhere'
pw = 'inertpwhere'

def createcredsapi(bucketpath, iamrole):
  url = host
  payload = {"name": "{}access".format(bucketpath), "aws_iam_role": {"role_arn": "{}".format(iamrole)}}
  r = requests.post(url, auth=HTTPBasicAuth(un, pw), json=payload)
  results = r.json()
  print(results)

# COMMAND ----------

#This function iterates through our buckets and calls the credentials API to create a storage credential that contains the iamrole that has the relevant permissions for that bucket. These credentials will be used for our external locations

def createcreds(bucketlist):
    for (bucketpath, iamrole) in zip(bucketlist['bucketroot'], bucketlist['iamrole']):
          createcredsapi(bucketpath, iamrole)   

# COMMAND ----------

#This function iterates through our buckets and creates an external location for each root bucket directory leveraging the credentials created above. These external locations will allow us to upgrade our external hive tables to unity without needing to include the credential in the SQL DDL for the create table commands

def createexternallocation(bucketlist):
    for br in bucketlist['bucketroot']:
        try:
          sql('CREATE EXTERNAL LOCATION `{}location` URL "s3://{}" WITH (STORAGE CREDENTIAL `{}access`)'.format(br, br, br))
        except:
          print('externallocationalreadyexists')
        else: 
          print('externallocationscreated')
    

# COMMAND ----------

# This function iterates through external HMS tables and creates a new external table in Unity in the catalog that we specify in the function. Since the external table is just pointing to a directory in s3 no data will actually have to be copied, saving the customer lots of time and effort. The great thing is that the customer can continue to point their pipelines to write to HMS but the new writes will still synch over to the Unity table, that way they can gradually update their pipelines over time. Also if they drop table in HMS the Unity table will persist. However, we would still want to be cautious before recomending that they drop HMS tables at scale since many of these tables are managed and would delete the underlying data as well.


def createtablesexternal(bucketlist):
    catalog = 'migrationtestc'
    for (schematable, location) in zip(bucketlist['name'], bucketlist['location']):
      sql('CREATE TABLE {}.{} LIKE {} LOCATION "{}"'.format(catalog, schematable, schematable, location))
      print('externaltablescreated')
    

# COMMAND ----------

#For dbfs tables we create a dataframe slice that will feed into subsequent functions. The user can skip steps 13-15 if they do not have any external tables and instead skip to cell 16.
dbfscta = maindfdbfs['name']
print(dbfscta)

# COMMAND ----------

#This function iterates through dbfs tables and clones those tables into the root s3 bucket specified when creating Unity. This function will need to be vectorized to work at scale-- also cloning can only be done if the underyling table is in Delta format, otherwise they will have to use the below function.

def createtablesdbfsdelta(bucketlist):
    catalog = 'migrationtest'
    for schmeatable in bucketlist:
        sql('CREATE TABLE IF NOT EXISTS {}.{} DEEP CLONE {}'.format(catalog, schematable, schematable))
        print('tablecloned')
    

# COMMAND ----------

#This function iterates through dbfs tables and recreates those tables into the root s3 bucket specified when creating Unity. This function will need to be vectorized to work at scale-- this function is only recomended if the underlying table is not in Delta format.

def createtablesdbfsnondelta(bucketlist):
    catalog = 'migrationtest'
    for hivetables in bucketlist:
        sql('CREATE TABLE IF NOT EXISTS {}.{} AS SELECT * FROM {}'.format(catalog, schematable, schematable))
        print('tablecopied')
    

# COMMAND ----------

#uncomment the following three lines if you have external tables in HMS
#createcreds(bucketpathdf)
#createexternallocation(bucketpathdf)
#createtablesexternal = createtablesexternal(s3bucketpd)

#uncomment the following line if you have dbfs delta tables in HMS
#createtablesdbfsdelta(dbfscta)

#uncomment the following line if you have dbfs non-delta tables in HMS
#createtablesdbfsnondelta(dbfscta)
