import sys
import warnings
import pyodbc
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from sqlalchemy import text
from myCountryModelPackages.sqlTableRetrieve import *

class CountryEconomicResearch:
    economic_data = None
    connection = None
    year = None

    query_economic_research = \
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
        # sqlQuery = f"{self.query_economic_research}{self.year}'"
        # self.economic_data = pd.read_sql(sqlQuery, self.connection)
        # self.economic_data = self.economic_data.rename(columns={'Total': 'Size'})
        # self.economic_data = self.economic_data.drop(['RangeDate', 'AutomationDegree'], axis=1)
        self.economic_data = EconomicResearchData(self.year).get_EconomicResearch()

# Generates the GDP table used for the Economic Research.  It does require a List of Countries that are to be used in each region
# The reason for the list of countries is so that we can exclude countries that are NOT used in the Country Model.
# The countries that are excluded, their GDP will be added to the Rest of "region" GDP.
class GDP_X_COUNTRY:
    country_gdp = None
    country_gdp_remainder = None
    country_gdp_annual = None
    region_gdp_annual = None

    connection = None
    year = None


    def get_country_gdp(self):
        return self.country_gdp_annual

    def __init__(self, year, country_list):
        self.connection = sql_MiraIndustry_Connection().get_connection()
        self.year = year
        sql_statement = \
            f"SELECT [Year],[Region],[Country],[Revenues],[Quarter] FROM [dbo].[FinancialMetrics_Country] " \
            f"WHERE [Year] = '{self.year}' " \
            f"ORDER BY [Country] "
        self.country_gdp = pd.read_sql(sql_statement, self.connection)
        country_set = set(country_list.apply(tuple, axis=1))
        self.country_gdp = self.country_gdp.loc[self.country_gdp[['Region','Country']].apply(tuple, axis=1).isin(country_set)]

        sql_statement = \
            f"SELECT [Year],[Region],[Country],[RemainderSize] FROM [dbo].[FinancialMetrics_GDP_Region_Remainder] " \
            f"WHERE [Year] = '{self.year}' " \
            f"ORDER BY [Country] "
        self.country_gdp_remainder = pd.read_sql(sql_statement, self.connection)
        self.country_gdp_remainder['Region'] = self.country_gdp_remainder['Region'].str.strip()

        #calculate the annual GDP table with remainder calculations to estimate the Rest of 'each region'
        self.country_gdp_annual = self.country_gdp.groupby( ['Year','Region','Country'])['Revenues'].sum().reset_index()
        self.region_gdp_annual = self.country_gdp_annual.groupby(['Year', 'Region']).sum().reset_index()
        self.region_gdp_annual = self.region_gdp_annual.drop(['Country'], axis=1)
        _gdp_region_remainder = self.country_gdp_remainder.groupby(['Year', 'Region'])['RemainderSize'].sum().reset_index()
        self.region_gdp_annual = self.region_gdp_annual.merge(_gdp_region_remainder, on = ['Year', 'Region'], how='left')
        self.region_gdp_annual ['RemainderSize'] = self.region_gdp_annual ['RemainderSize'].fillna(0)
        self.region_gdp_annual['RegionTotal'] = self.region_gdp_annual ['Revenues'] / (1 - self.region_gdp_annual ['RemainderSize'])
        self.region_gdp_annual = self.region_gdp_annual.drop(['Revenues', 'RemainderSize'], axis=1)

        # Compute the Rest of xxx GDP and add to the Country GDP table.
        self.country_gdp_annual = pd.concat([self.country_gdp_annual,self.country_gdp_remainder])
        self.country_gdp_annual = self.country_gdp_annual.merge(self.region_gdp_annual, on = ['Year', 'Region'], how='left')
        self.country_gdp_annual = self.country_gdp_annual.fillna(0)
        self.country_gdp_annual['GDP'] = self.country_gdp_annual['Revenues'] + (self.country_gdp_annual['RemainderSize']*self.country_gdp_annual['RegionTotal'])
        self.country_gdp_annual = self.country_gdp_annual.drop(['Revenues','RemainderSize','RegionTotal'], axis=1)
        self.country_gdp_annual['Region'] = self.country_gdp_annual['Region'].str.strip()
        self.country_gdp_annual['Country'] = self.country_gdp_annual[('Country')].str.strip()

# The Economic Research is generated dynamically in this class.
#  Required are two tables:
#       CountryModel_AutomationDegree
#           IndustryFraction = for each country list of industries and what weight is in the country
#           Automation Degree  = what degree of automation in that industry/country combination
#       CountryModel_IndustryGDP
#           IndustrialGDP_Fraction  =  GDP by country is a fractional multiplier of the total country GDP
class EconomicResearchData:
    year = None
    country_model_industry_gdp_fraction = None
    country_model_industry_automation_degree = None
    economic_research_data = None
    gdp_x_country = None


    def get_Economic_Comparison(self):
        economic_research_compare = None
        db_cxcn_economic_research = sql_MiraLite_connection().get_connection()
        economic_research = CountryEconomicResearch(db_cxcn_economic_research, self.year).get_EconomicTable()
        economic_research_compare = economic_research.merge(self.economic_research_data,on = ['BaseYear', 'Region','Country','Industry'], how='left')
        economic_research_compare['Delta'] = abs(economic_research_compare['CountryWeight_x'] - economic_research_compare['CountryWeight_y'])
        return economic_research_compare

    def get_EconomicResearch(self):
        return self.economic_research_data

    def get_industrial_gdp_fraction(self):
        return self.country_model_industry_gdp_fraction

    def get_industrial_automation_degree(self):
        return self.country_model_industry_automation_degree

    def __init__(self, year):
        self.connection = sql_MiraIndustry_Connection().get_connection()
        self.year = year

        sql_statement_industryGDP = \
            f"SELECT [BaseYear],[Region],[Country],[IndustrialGDP_Fraction] FROM [dbo].[CountryModel_IndustryGDP] " \
            f"WHERE [BaseYear] = '{self.year}' " \
            f"ORDER BY [Country] "
        self.country_model_industry_gdp_fraction = pd.read_sql(sql_statement_industryGDP, self.connection)
        self.country_model_industry_gdp_fraction['Region'] = self.country_model_industry_gdp_fraction['Region'].str.strip()
        self.country_model_industry_gdp_fraction['Country'] = self.country_model_industry_gdp_fraction['Country'].str.strip()

        country_list = self.country_model_industry_gdp_fraction.drop(['BaseYear', 'IndustrialGDP_Fraction'], axis=1)
        self.gdp_x_country = GDP_X_COUNTRY(year, country_list).get_country_gdp()
        self.gdp_x_country.rename(columns={'Year': 'BaseYear'}, inplace=True)
        sql_statement_automation_degree = \
            f"SELECT [BaseYear],[Region], [Country],[Industry],[AutomationDegree], [IndustryFraction] FROM [dbo].[CountryModel_AutomationDegree] " \
            f"WHERE [BaseYear] = '{self.year}' " \
            f"ORDER BY [Region], [Country] "
        self.country_model_industry_automation_degree = pd.read_sql(sql_statement_automation_degree, self.connection)
        self.country_model_industry_automation_degree['Region'] = self.country_model_industry_automation_degree['Region'].str.strip()
        self.country_model_industry_automation_degree['Country'] = self.country_model_industry_automation_degree['Country'].str.strip()
        self.country_model_industry_automation_degree['Industry'] = self.country_model_industry_automation_degree['Industry'].str.strip()
        self.economic_research_data = self.gdp_x_country.merge(self.country_model_industry_gdp_fraction,on = ['BaseYear', 'Region','Country'], how='left')
        self.economic_research_data ['IndustrialGDP_Fraction'] = self.economic_research_data ['IndustrialGDP_Fraction'].fillna(1)
        self.economic_research_data = self.economic_research_data.merge(self.country_model_industry_automation_degree,on = ['BaseYear', 'Region','Country'], how='left')
        self.economic_research_data['CountryWeight'] = self.economic_research_data['GDP'] * self.economic_research_data['IndustrialGDP_Fraction'] * self.economic_research_data['IndustryFraction']* self.economic_research_data['AutomationDegree']
        self.economic_research_data = self.economic_research_data.drop(['GDP', 'IndustrialGDP_Fraction', 'AutomationDegree'], axis=1)
