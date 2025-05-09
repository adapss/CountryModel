import sys
import warnings
import pyodbc
import pandas as pd
import numpy as np

warnings.filterwarnings('ignore')
# This class is used to pull a list of market reports available in the database.
#  There are two tables that can be retrieved
#       base_year_list  - this just provides a comprehensive list of base years that are in the database
#       market_reports - this is a dataframe with Report Name and associated Base Year

class CountryModelEvaluation:
    cxcn = None

    def compare_country_model_size(self, country_model_size, sql_country_model_size):
        sql_country_model_size = sql_country_model_size.rename(columns={'Size': 'Size_sql'})
        country_model = pd.merge(country_model_size, sql_country_model_size, on=['Study','BaseYear','Segment','Region','Company','Industry'], how='outer')
        country_model['Delta'] = country_model['Size']- country_model['Size_sql']
        return country_model

    def compare_country_model_forecast(self, country_model_forecast, sql_country_model_forecast):
        sql_country_model_forecast = sql_country_model_forecast.rename(columns={'Forecast': 'Forecast_sql'})
        country_model = pd.merge(country_model_forecast, sql_country_model_forecast, on=['Study','BaseYear','Year','Segment','Region','Country','Industry'], how='outer')
        country_model['Delta'] = country_model['Forecast'] - country_model['Forecast_sql']
        return country_model

    def __init__(self,cxcn):
        self.connection = cxcn
