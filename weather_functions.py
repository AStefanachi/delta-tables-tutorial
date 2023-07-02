# Databricks notebook source
# DBTITLE 1,Imports
from datetime import datetime, date
import uuid
import pytz

# COMMAND ----------

# DBTITLE 1,Functions
def replace_dots_in_columns(sdf):
    """
    Replaces the . contained in the dataframe column names with an _ and returns a pyspark dataframe
    """
    for col in sdf.columns:
        sdf = sdf.withColumnRenamed(col, col.replace('.', '_'))
    return sdf

# COMMAND ----------

def run_metadata():
    """
    Returns a dictionary containing metadata about the latest Run according to the CET time zone
    """
    cet = pytz.timezone('CET')
    _run_date = str(date.today())
    _run_timestamp = str(datetime.now().replace(tzinfo=pytz.utc).astimezone(cet))
    _run_id = str(uuid.uuid1())
    return { "_run_date": _run_date, "_run_timestamp": _run_timestamp, "_run_id": _run_id }