# Databricks notebook source
# DBTITLE 1,Constants
SECRET_SCOPE = "kwdwe001"

# List of cities
CITIES = ["hamburg", "berlin", "bremen", "frankfurt"]

# Api key from weatherapi.com
API_KEY = dbutils.secrets.get(SECRET_SCOPE, "weather-api-key")

# COMMAND ----------

# DBTITLE 1,Secrets
STORAGE_ACCOUNT = dbutils.secrets.get(SECRET_SCOPE, "sta-d-we-001-acc")

STORAGE_ACCOUNT_NAME = dbutils.secrets.get(SECRET_SCOPE, "sas-token-sta-d-001-name")

STORAGE_ACCOUNT_SAS_TOKEN = dbutils.secrets.get(SECRET_SCOPE, "sas-token-sta-d-001")

DLS_CONTAINER = "dls"