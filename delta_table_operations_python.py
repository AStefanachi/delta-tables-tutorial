# Databricks notebook source
# MAGIC %md
# MAGIC These code snippets will show you the features of using the delta tables.
# MAGIC - https://github.com/delta-io/delta/blob/master/examples/cheat_sheet/delta_lake_cheat_sheet.pdf
# MAGIC
# MAGIC For more information about the commands used, visit below page:
# MAGIC - https://docs.delta.io/latest/index.html

# COMMAND ----------

# DBTITLE 1,Constants
# MAGIC %run ./weather_constants

# COMMAND ----------

# DBTITLE 1,Settings SAS Token
# Define the Delta table name and path
TABLE_NAME =  "current"
TABLE_PATH = f"wasbs://{DLS_CONTAINER}@{STORAGE_ACCOUNT_NAME}.blob.core.windows.net/" \
    f"weather_api/current/{TABLE_NAME}"

# Setting SAS Configuration
spark.conf.set(f"fs.azure.account.auth.type.{STORAGE_ACCOUNT_NAME}.blob.core.windows.net", "SAS")
spark.conf.set(f"fs.azure.sas.{DLS_CONTAINER}.{STORAGE_ACCOUNT_NAME}.blob.core.windows.net", STORAGE_ACCOUNT_SAS_TOKEN)

# COMMAND ----------

# DBTITLE 1,Anatomy of the delta table
# According to the cluster configuration, spark creates snappy compressed parquet files.
# In my case, having 4 cores and a limited dataset, I have a number of files multiple of 4
fi_path = [fi.path for fi in dbutils.fs.ls(TABLE_PATH)]
fi_path

# COMMAND ----------

# The _delta_log folder contains transaction information and metadata about the table
# every 10 commits a checkpoint file will be generated
# more info here: https://www.databricks.com/blog/2019/08/21/diving-into-delta-lake-unpacking-the-transaction-log.html
fi_path_delta_log = [fi.path for fi in dbutils.fs.ls(TABLE_PATH + "/_delta_log")]
fi_path_delta_log

# COMMAND ----------

# DBTITLE 1,Transaction log
# What's inside a transaction file?
spark.read.json(f"wasbs://dls@{STORAGE_ACCOUNT_NAME}.blob.core.windows.net/weather_api/current/current/_delta_log/00000000000000000018.json").display()

# COMMAND ----------

# DBTITLE 1,Checkpoint file
# What's inside a checkpoint file?
spark.read.parquet(f"wasbs://dls@{STORAGE_ACCOUNT_NAME}.blob.core.windows.net/weather_api/current/current/_delta_log/00000000000000000010.checkpoint.parquet").display()

# COMMAND ----------

# DBTITLE 1,Reading a delta table into a spark dataframe
current_forecast = spark.read.table('bronze_weather.current')
current_forecast.display()

# COMMAND ----------

# DBTITLE 1,Read delta table as DeltaTable object
from delta.tables import DeltaTable
deltaTable = DeltaTable.forName(spark, 'bronze_weather.current')
# Convert the Delta Table into a spark data frame and display the content
deltaTable.toDF().display()

# COMMAND ----------

# DBTITLE 1,Describe Detail
# information about the table such as size, name, location (for external tables)
deltaTable.detail().display()

# COMMAND ----------

# DBTITLE 1,Deleting rows according to a SQL formatted condition
# We simulate an human error, which will delete all records
deltaTable.delete("location_localtime <= '2023-07-19 6:52'")

# COMMAND ----------

# DBTITLE 1,Confirming Deletion
deltaTable.toDF().select('location_localtime').where("location_localtime < '2023-07-19 6:52'").count()

# COMMAND ----------

# DBTITLE 1,Display table version history to identify transactions
deltaTable.history().display()

# COMMAND ----------

# DBTITLE 1,Checking differences between table versions
# Version 18: before deleting the 19/07/2023
# Version 19: after delete
version19 = spark.read.option('versionAsOf', 19).table('bronze_weather.current')
version18 = spark.read.option('versionAsOf', 18).table('bronze_weather.current')

print("Records which have been deleted between version 19 and 18 \n\r")
version18.exceptAll(version19).display()

# COMMAND ----------

# DBTITLE 1,Restore table as of a precedent version
deltaTable.restoreToVersion(18)