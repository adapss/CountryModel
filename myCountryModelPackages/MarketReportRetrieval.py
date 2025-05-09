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

class MarketReports:
    market_reports = None
    base_year_list = None

    def get_report_list(self):
        return self.market_reports

    def get_base_year_list(self):
        return self.market_reports['BaseYear'].drop_duplicates()

    def __init__(self,cxcn ):
        worldSegment = "World Region"
        industrySegment = "Industry"
        categoryName = "%"
        marketStudy = "%"
        year = "%"
        self.connection = cxcn
        self.sql_query_market_size = \
            f"SELECT [Study], [BaseYear] FROM [dbo].[StudyForecasts]" \
            f"WHERE   ([Study] LIKE '{marketStudy}') AND ([BaseYear] LIKE  '{year}') AND (([Segment] LIKE '{worldSegment}')  OR ([Segment] LIKE '{industrySegment}'))" \
            f"AND ([Category] LIKE '{categoryName}')" \
            f"ORDER BY [BaseYear]"
        self.market_reports = pd.read_sql(self.sql_query_market_size, self.connection)
        self.market_reports = self.market_reports[['BaseYear', 'Study']].drop_duplicates()
        self.base_year_list = self.market_reports['BaseYear'].drop_duplicates()

# MarketReportData class is used to pull market report data from either the worldwide or country model tables in the SQL database.
# Currently, it only allows for one market report retrieval at a time.  Although this could easily be extended.
class MarketReportData:
    market_report = None
    base_year= None

    def get_worldwide_size(self):
        sql_query_market_size = \
            f"SELECT [Study], [BaseYear],[Company],[Segment],[Category],[ParentCategory],[Size] FROM [dbo].[StudySizes]" \
            f"WHERE   ([Study] = '{self.market_report}') AND ([BaseYear] = '{self.base_year}') " \
            f"ORDER BY  [Company]"
        model_data = pd.read_sql(sql_query_market_size, self.connection)
        return model_data

    def get_worldwide_forecast(self):
        sql_query_market_forecast = \
            f"SELECT [Study], [BaseYear],[Year],[Segment],[Category],[ParentCategory],[GrandParentCategory],[Forecast] FROM [dbo].[StudyForecasts] " \
            f"WHERE   ([Study] = '{self.market_report}') AND ([BaseYear] = '{self.base_year}') " \
            f"ORDER BY  [Segment], [Category], [Year]"
        model_data = pd.read_sql(sql_query_market_forecast, self.connection)
        return model_data


    def get_country_model_size(self):
        sql_query_market_size = \
            f"SELECT [Study], [BaseYear],[Company],[Segment],[Category],[ParentCategory],[Size] FROM [dbo].[StudySizesCountryModel]" \
            f"WHERE   ([Study] = '{self.market_report}') AND ([BaseYear] = '{self.base_year}') " \
            f"ORDER BY  [Company]"
        country_model_data = pd.read_sql(sql_query_market_size, self.connection)
        country_model_data = country_model_data.rename(columns={'Category': 'Region', 'ParentCategory': 'Industry'})
        return country_model_data

    def get_country_model_forecast(self):
        sql_query_market_forecast = \
            f"SELECT [Study], [BaseYear],[Year],[Segment],[Category],[ParentCategory],[GrandParentCategory],[Forecast] FROM [dbo].[StudyForecastsCountryModel] " \
            f"WHERE   ([Study] = '{self.market_report}') AND ([BaseYear] = '{self.base_year}') " \
            f"ORDER BY  [Segment], [Category], [Year]"
        country_model_data = pd.read_sql(sql_query_market_forecast, self.connection)
        country_model_data = country_model_data.rename(columns={'Category': 'Region', 'ParentCategory': 'Country','GrandParentCategory': 'Industry'})
        return country_model_data

    def __init__(self,cxcn, market_report, base_year ):
        self.connection = cxcn
        self.market_report = market_report
        self.base_year = base_year
