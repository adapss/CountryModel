import sys
import warnings
import pyodbc
import pandas as pd
import numpy as np

from myCountryModelPackages.MarketReportRetrieval import *
from myCountryModelPackages.Economic_Research import *

warnings.filterwarnings('ignore')

class Country_Model_Forecast:
    market_forecast_region = None
    market_forecast_industry = None
    region_list = None
    industry_list = None
    base_year = None
    gCC = None
    gRC = None


    def get_sRxIp_Table(self):
        # CMF Step-1
        _ww_by_year = self.market_forecast_region.groupby(['Year'])['Forecast'].sum().reset_index()
        _all_industry_by_year = self.market_forecast_industry.groupby(['Year'])['Forecast'].sum().reset_index()
        _all_industry_remainder = _ww_by_year.merge(_all_industry_by_year, on=['Year'], how='left')
        _all_industry_remainder['Industry_Remainder'] = _all_industry_remainder['Forecast_x'] - _all_industry_remainder[
            'Forecast_y']
        _all_industry_remainder = _all_industry_remainder.drop(['Forecast_x','Forecast_y'], axis=1)

        # CMF Step-2
        industry_by_region_year = self.market_forecast_industry.merge(self.gRC, on=['Industry'], how='left')
        industry_by_region_year['Forecast_Industry_X_Region'] = industry_by_region_year['gRC_Size'] * industry_by_region_year['Forecast']
        industry_by_region_year = industry_by_region_year.drop(['gRC_Size','Forecast'], axis=1)
        region_by_year = industry_by_region_year
        region_by_year['Region_Total'] = industry_by_region_year.groupby(['Region', 'Year'])['Forecast_Industry_X_Region'].transform('sum')
        sRxIp = pd.merge(industry_by_region_year, _all_industry_remainder, on=['Year'])
        sRxIp = pd.merge(sRxIp, self.gRC.loc[self.gRC['Industry'] == 'Other Industries'], on='Region')
        sRxIp['Size'] = sRxIp['Forecast_Industry_X_Region'] / (sRxIp['Region_Total'] + (sRxIp['Industry_Remainder'] * sRxIp['gRC_Size']))
        sRxIp = sRxIp.drop(['gRC_Size','Forecast_Industry_X_Region','Region_Total','Industry_Remainder','Industry_y'], axis=1)
        return  sRxIp

    def __init__(self, market_forecast_region, market_forecast_industry, base_year, gRC, gCC , regions, industries):
        self.region_list = regions
        self.industry_list = industries
        self.market_forecast_industry = market_forecast_industry
        self.market_forecast_region = market_forecast_region
        self.market_forecast_industry = self.market_forecast_industry.loc[self.market_forecast_industry['Industry'].isin(self.industry_list),['Industry', 'Forecast', 'Year']]
        self.base_year = base_year
        self.gCC = gCC
        self.gRC = gRC
        self.gRC = self.gRC.rename(columns={'Size': 'gRC_Size'})


class Report_CountryKnowns_Market_Forecast:
    country_known_forecast = None
    connection = None
    sql_queryCountryKnown = None
    year = None

    def get_CountryKnown_Table(self):
        return self.country_known_forecast

    def get_kRxI(self, kCxIp):
        return

    #CMF-Step-3
    # ** Remove country known from region revenues
    # **   and distribute remainder over industries
    def get_ukRxI(self,market_ww_forecast_x_region, sRxIp, region_x_country_model):

        # create a list of Regions where all the Countries are Known
        # We will exclude those Regions;  the reason is that the calculation that removes the Country Known forecast
        # has a slight numerical difference between the region total from the Country Known table and the Worldwide table.
        # This makes it cleaner.
        _ckList = self.country_known_forecast.loc[:, ['Region', 'Country']].drop_duplicates()
        _ckList = pd.merge(  region_x_country_model, _ckList, on=['Region','Country'], how='outer', indicator=True)
        _regions_without_all_known_countries = _ckList[_ckList['_merge'] != 'both']
        _regions_without_all_known_countries = _regions_without_all_known_countries.drop(['Country','_merge'],axis=1).drop_duplicates()
        _regionList = _regions_without_all_known_countries['Region'].tolist()

        _kRegion_total_x_year = self.country_known_forecast.groupby(['Region','BaseYear','Year'])['Forecast'].sum().reset_index()
        _kRegion_total_x_year = _kRegion_total_x_year.rename(columns={'Forecast': 'Region_Total'})

        _ukRxI =  _kRegion_total_x_year.merge(market_ww_forecast_x_region, on=['Region','Year'], how='left')
        _ukRxI = _ukRxI[_ukRxI['Region'].isin(_regionList )]

        _ukRxI = _ukRxI.merge(sRxIp,on=['Region','Year'],how='left')
        # Remove the Country Known Forecast
        _ukRxI['Remove_CK_Forecast'] = _ukRxI['Size'] * (_ukRxI['Forecast'] - _ukRxI['Region_Total'])
        _ukRxI = _ukRxI.drop(['Forecast','Size','Region_Total'], axis=1)
        _ukRxI = _ukRxI.rename(columns={'Remove_CK_Forecast': 'Forecast', 'Industry_x': 'Industry'})

        return _ukRxI

    # CMF-Step-4
    # ** Compute Country percent of Rest of Region by Industry.
    # **   Ignoring countries with any knowns and industries with no data.
    # **   First compute total of country research by industry
    # **     for each region, ignoring known countries.
    def get_ukCpRxI(self,gResearchCxI ):
        ukCpRxI = gResearchCxI.loc[~gResearchCxI['Country'].isin(self.country_known_forecast['Country'])]
        ukCpRxI['Region_Total'] =ukCpRxI.groupby(['Region','Industry'])['CountryWeight'].transform('sum')
        ukCpRxI['Size'] = ukCpRxI['CountryWeight']/ukCpRxI['Region_Total']
        ukCpRxI = ukCpRxI.drop(['CountryWeight','Region_Total'], axis=1)
        return ukCpRxI

    # CMF-Step-5
    # ** Compute the Country by Industry revenue matrix for unknown countries
    #  need two  dataframes previousl calcualted
    #       Step-3  ukRxI  (Region X Industry)
    #       Step-4  ukCpRxI (Country (%) X Industry)
    def get_ukCxI(self, ukRxI, ukCpRxI):
        # Left merge on ukRxI, ensures only the industries in the Market Report are included in the model.
        ukCxI = pd.merge( ukRxI, ukCpRxI,  on=['Region','Industry'], how='left')
        ukCxI['Country_Forecast'] = ukCxI['Size'] * ukCxI['Forecast']
        ukCxI = ukCxI.drop(['Size','Forecast'], axis=1)
        ukCxI = ukCxI.rename(columns={'Country_Forecast': 'Forecast'})
        return ukCxI

    # CMF-Step-6
    # ** Compute Country by Industry for country known values
    # **   Industry distribution matrix to use when only the country total is known
    #     If the analyst provided details on the Industry breakdown for the specific country, then we will not use this dataframe
    def get_kCxIp(self, ww_forecast_x_industry, gCC):
        _gCC = gCC.loc[gCC['Country'].isin(self.country_known_forecast['Country'])]
        kCxIp = pd.merge(ww_forecast_x_industry,_gCC,on=['Industry'], how='left')
        kCxIp ['Industry_x_Country'] =  kCxIp ['Forecast'] *  kCxIp ['Size']
        kCxIp = kCxIp.drop(['Segment','Forecast','Size'],axis=1)
        kCxIp['Country_Total'] = kCxIp.groupby(['BaseYear', 'Year','Country'])['Industry_x_Country'].transform('sum')
        kCxIp ['Forecast'] = kCxIp ['Industry_x_Country'] / kCxIp['Country_Total']
        kCxIp = kCxIp.drop(['Industry_x_Country','Country_Total'],axis=1)
        return kCxIp

    # CMF-Step-7
    # ** Assemble the country known dataframe and configure it to be merged with unknown country forecast table.
    # This algorithm assumes that only the "Other Industries", is present.
    # **   Segmenting by industry using the country industry character.

    def get_kCxI (self,kCxIp):
        kCxI = pd.merge(self.country_known_forecast.loc[self.country_known_forecast['Industry'] == "Other Industries"],kCxIp,on=['BaseYear','Year','Region','Country'], how='left')
        kCxI['Forecast'] = kCxI['Forecast_x'] * kCxI['Forecast_y']
        kCxI = kCxI.drop(['Forecast_x','Forecast_y','Industry_x'],axis=1)
        kCxI = kCxI.rename(columns={'Industry_y': 'Industry'})
        kCxI.insert(0, 'Segment', 'World Region')
        return kCxI

    def get_Merge_Knowns(self, ukCxI, kCxI):
        # forecast = pd.merge(ukCxI, kCxI, on=['BaseYear','Year','Region','Country','Industry'], how='left')
        forecast = pd.concat([ukCxI, kCxI])
        return forecast

    def get_country_known_list(self,economic_research):
        countries = economic_research.get_CountryList()
        return self.country_known_forecast.loc[self.country_known_forecast['Country'].isin(countries), 'Country'].drop_duplicates()

    def __init__(self,cxcn, marketStudy,year,economic_research):
        gRegionList = economic_research.get_RegionList()
        gRegion_x_Country = economic_research.get_Region_X_Country_Table()
        self.connection = cxcn
        self.year = year
        self.sql_queryCountryKnown = f"SELECT [Study], [BaseYear],[Year], [Country],[Industry], [Forecast] FROM [dbo].[CountryForecasts]" \
                            f" WHERE   (([Study] = '{marketStudy}') AND ([BaseYear] =  {year}))" \
                            f" ORDER BY [Study], [BaseYear],[Year],[Country], [Industry]"
        self.country_known_forecast = pd.read_sql(self.sql_queryCountryKnown, self.connection)
        self.country_known_forecast = self.country_known_forecast.merge(gRegion_x_Country, on='Country', how='left')
        self.country_known_forecast = self.country_known_forecast[self.country_known_forecast['Region'].isin(gRegionList)]
        self.country_known_forecast = self.country_known_forecast.drop(['Study'],axis=1)







