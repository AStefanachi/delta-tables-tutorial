# Databricks notebook source
# DBTITLE 1,Imports
import requests

# COMMAND ----------

# DBTITLE 1,Classes
class WeatherAPI:
    def __init__(self, api_key, city):
        """
        Initialize 3 variables:
        - api_key: secret API key provided by weatherapi.com
        - city: parameter that need to be passed in lowercase
        - API_URL: fixed value
        """
        self.api_key = api_key
        self.city = city
        self.API_URL = "http://api.weatherapi.com/v1/"
    
    def get_json_response(self, endpoint):
        """
        Passing the parameters to the endpoint inclusive of the .json extension
        will return a json formatted response
        """
        request = self.API_URL + endpoint
        params = {
            "key": self.api_key,
            "q": self.city
        }
        response = requests.get(request, params=params)
        return response.json()