import sys
import warnings
import pyodbc
import pandas as pd
import numpy as np

class CountryEconomicResearch:
    economic_data = None
    connection = None
    year = None

    query_economic_reserch = \
        "SELECT [BaseYear],[Country],[Region],[Industry],[AutomationDegree],[IndustryFraction], [CountryWeight],[RangeDate] FROM [dbo].[CountryEconomicData]" \
        " WHERE [SetName] = '" + "Economic Model" + "' " \
        " AND [BaseYear]= '"  #+ baseYear + "'"

    def get_EconomicTable(self):
        return self.economic_data

    def get_gResearchCxI(self):
        gResearchCxI = self.economic_data.groupby(['Region','Country','Industry'])['CountryWeight'].sum().reset_index()
        # gResearchCxI = gResearchCxI.drop(['CountryWeight'], axis=1)
        return gResearchCxI

    def get_gRC_region_character(self):
        # 2a) Build a Regional Character Table -gRC
        _industry_ww_totals = self.economic_data.groupby('Industry')['CountryWeight'].sum().reset_index()
        _industry_ww_totals.columns = ['Industry', 'TotalCountryWeight']
        _industry_region_total = self.economic_data.groupby(['Region', 'Industry'])['CountryWeight'].sum().reset_index()
        # Merge the groupby results to get the total CountryWeight for each Industry
        gRC = _industry_region_total.merge(_industry_ww_totals, on='Industry', how='left')
        # Calculate the ratio of CountryWeight for each Region and Industry combination
        gRC['Size'] = gRC['CountryWeight'] / gRC['TotalCountryWeight']
        gRC = gRC.drop(['CountryWeight', 'TotalCountryWeight'], axis=1)
        return gRC

    def get_gCC_country_character(self):
        # 2a) Build a Country Character Table -gCC
        _industry_country_totals = self.economic_data.groupby(['Country'])['CountryWeight'].sum().reset_index()
    #    _industry_country_totals.columns = ['Country','Industry', 'TotalCountryWeight']
        _industry_country_total = self.economic_data.groupby(['Country'])['CountryWeight'].sum().reset_index()
        # Merge the groupby results to get the total CountryWeight for each Industry
        gCC = self.economic_data.merge(_industry_country_totals, on='Country', how='left')
        # Calculate the ratio of CountryWeight for each Region and Industry combination
        gCC['Size'] = gCC['CountryWeight_x'] / gCC['CountryWeight_y']
        gCC = gCC.drop(['CountryWeight_x', 'CountryWeight_y','IndustryFraction'], axis=1)
        return gCC

    def get_Region_X_Country_Table(self):
        gRxC = self.economic_data.loc[:, ['Region', 'Country']].drop_duplicates()
        # gRxC['key'] = gRxC['Region'] + "~" + gRxC['Country']
        return gRxC

    def get_Country_X_IndustryTable(self):
        gCxI = self.economic_data.loc[:, ['Region', 'Country', 'Industry', 'CountryWeight']]
        gCxI['key'] = gCxI['Country'] + "~" + gCxI['Industry']
        return gCxI

    def get_IndustryList(self):
        return self.economic_data['Industry'].drop_duplicates()

    def get_RegionList(self):
        return self.economic_data['Region'].drop_duplicates()

    def get_CountryList(self):
        return self.economic_data['Country'].drop_duplicates()

    def getConnection(self):
        print(self.myConnection)

    def __init__(self,cxcn,year):
        self.connection = cxcn
        self.year = year
        sqlQuery = f"{self.query_economic_reserch}{self.year}'"
        self.economic_data = pd.read_sql(sqlQuery, self.connection)
        self.economic_data = self.economic_data.rename(columns={'Total': 'Size'})
        self.economic_data = self.economic_data.drop(['RangeDate', 'AutomationDegree'], axis=1)
