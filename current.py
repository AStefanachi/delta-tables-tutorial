# Databricks notebook source
# MAGIC %md
# MAGIC # Ingesting API Response in the bronze layer as Delta Table

# COMMAND ----------

# DBTITLE 1,Weather constants
# MAGIC %run ./weather_constants

# COMMAND ----------

# DBTITLE 1,Weather classes
# MAGIC %run ./weather_classes

# COMMAND ----------

# DBTITLE 1,Weather functions
# MAGIC %run ./weather_functions

# COMMAND ----------

# DBTITLE 1,Imports
from pyspark.sql import functions as F
import pandas as pd
from functools import reduce

# COMMAND ----------

# DBTITLE 1,Normalize the json response into a list of cleaned dataframes
cities_current_weather = [
        replace_dots_in_columns(
            spark.createDataFrame(
                pd.json_normalize(
                    WeatherAPI(API_KEY, city).get_json_response("current.json")
                    )
                ) 
        )
    for city in CITIES
]

# COMMAND ----------

# DBTITLE 1,Reduce into a single dataframe
cities_df_union = reduce(lambda acc, curr: acc.union(curr), cities_current_weather)

# COMMAND ----------

# DBTITLE 1,Adding the run_metadata() information via a crossJoin to the cities_df_union dataframe
metadata_df = pd.DataFrame([run_metadata()])
metadata_df = spark.createDataFrame(metadata_df)
cities_df_union = cities_df_union.crossJoin(metadata_df)

# COMMAND ----------

# DBTITLE 1,Check if the schema exists, otherwise create
spark.sql("CREATE SCHEMA IF NOT EXISTS bronze_weather")

# COMMAND ----------

# DBTITLE 1,Create delta table on a storage account in append mode to build history
# Define the Delta table name and path
TABLE_NAME =  "current"
TABLE_PATH = f"abfss://{DLS_CONTAINER}@{STORAGE_ACCOUNT_NAME}.blob.core.windows.net/" \
    f"weather_api/current/{TABLE_NAME}"

# Setting SAS Configuration
spark.conf.set(f"fs.azure.account.auth.type.{STORAGE_ACCOUNT_NAME}.blob.core.windows.net", "SAS")
spark.conf.set(f"fs.azure.sas.{DLS_CONTAINER}.{STORAGE_ACCOUNT_NAME}.blob.core.windows.net", STORAGE_ACCOUNT_SAS_TOKEN)

# Selecting df in order
cities_df_union = cities_df_union.select(cities_df_union.columns)

# Write the DataFrame as a Delta table to the external storage account
cities_df_union.write \
  .option('path', TABLE_PATH) \
  .mode('append') \
  .saveAsTable('bronze_weather.current')