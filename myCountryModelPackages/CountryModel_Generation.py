from email.policy import default
from itertools import groupby
import sys
import pyodbc
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import warnings
from sqlalchemy import create_engine

from myCountryModelPackages.sqlTableRetrieve import *
from myCountryModelPackages.MarketReportRetrieval import *
from myCountryModelPackages.CountryModel_Forecast import *
from myCountryModelPackages.CountryModel_MarketSize import *
from myCountryModelPackages.Economic_Research import *

class Country_Model_Generation:
    marketStudy = "AC Drives Low Voltage"
    baseYear:str = "2023"
    dump = False
    db_cxcn = None

    def generate_market_shares(self):
        # Economic Tables from Analyst Research
        #  - build Lists Countries, Industries and Regions included in the Economic analysis
        economic_research = CountryEconomicResearch(self.db_cxcn, self.baseYear)
        gIndustryList = economic_research.get_IndustryList()
        gRegionList = economic_research.get_RegionList()

        #Country Known Tables
        # The country Knows are derived from the WorldWide Market reports
        # Analysts will collect a select number of countries within the regions to override the modeling  of the Economic tables.
        CountryKnown_Size_Data = Report_CountryKnowns_Market_Size(self.db_cxcn, self.marketStudy, self.baseYear, economic_research)
        gCompanyCountryKnown = CountryKnown_Size_Data.get_CountryKnown_Table()

        # Worldwide Market Size Data Tables
        market_report_data = Report_Market_Size_Data(self.db_cxcn, self.marketStudy, self.baseYear)
        company_by_industry_base_year = market_report_data.get_MarketSize_X_Industry_Table(gIndustryList)
        company_by_region_base_year = market_report_data.get_MarketSize_X_Region_Table(gRegionList)

        # 2) Build Regional and Country Character Tables
        #       gRC - Regional Character Table
        #       gCC - Country Character Table -
        gRC = economic_research.get_gRC_region_character()
        gCC = economic_research.get_gCC_country_character()
        gResearchCxI = economic_research.get_gResearchCxI()

        # Country Model Market Share Generation
        #
        #3) Build study-Region_X_Industry table.
        #   - derived from Economic data and market study data
        #   - Economic research provides granularity down to the regional industries.
        sRxIp = market_report_data.get_sRxIp_Table(gRC)

        # Step 4-b Country Known Inclusion
        #  - Compute the Region totals by company derived from the Country Known Table.
        #  - build a table of
        ukRxI = CountryKnown_Size_Data.get_ukRxI(company_by_region_base_year, sRxIp)

        #'Step-4c
        #    '** Make list of known countries, counting the number of industries known.
        #   '**   If industry details are known, country total will be in "Other In_kRxI = gCompanyCountryKnown.loc[(gCompanyCountryKnown['Industry']=="Other Industries"),['Region','Country','Company','Industry','Size']]

        #     indRevForCountry = gProductByCompanyByIndustryByYr(indKey) * gCC(ciKey)
        # Build a dataframe identified as kCxIp  === known Country X Industry (%)
        kCxIp = CountryKnown_Size_Data.get_kCxIp(company_by_industry_base_year, gCC, economic_research)
        # Step-4d
        #    - Roll up CountryByIndustry knowns to RegionByIndustry,
        #    - computing industry values when only "Other Industries" is known
        # Build a "kRxI"  == known Region by Industry table. for each company
        #     kRxI  == [Company] [region] [Industry] [Size]
        #
        #   -Most reports only have the "Other Industries" identified so what we are going to do on first pass is
        #       redistribute the "Other Industries" for each of the Known countries based on the Economic Research
        #   - Second pass if a report has identified specific industries in the Country Known table then the algorithm will build the table with those.
        kRxI = CountryKnown_Size_Data.get_kRxI( kCxIp)

        # 'Step-5
        #    '** Assemble Regional Industry values computed with knowns
        country_model_market_share = ukRxI.merge(kRxI, on=['Company', 'Region','Industry'], how='left')
        country_model_market_share['Size'] = (country_model_market_share  ['Size_x'] + country_model_market_share ['Size_y'])
        country_model_market_share = country_model_market_share.drop(['Size_x','Size_y'],axis=1)
        country_model_market_share['Study']= self.marketStudy
        country_model_market_share['BaseYear']= self.baseYear
        country_model_market_share['Segment'] = "World Region"
        return country_model_market_share

    def generate_forecast(self):
       # Economic Tables from Analyst Research
        #  - build Lists Countries, Industries and Regions included in the Economic analysis
        economic_research = CountryEconomicResearch(self.db_cxcn, self.baseYear)
        gRegion_x_Country = economic_research.get_Region_X_Country_Table()
        gIndustryList = economic_research.get_IndustryList()
        gCountryList = economic_research.get_CountryList()
        gRegionList = economic_research.get_RegionList()

        market_report_data = Report_Market_Size_Data(self.db_cxcn, self.marketStudy, self.baseYear)

        # 2) Build Regional and Country Character Tables
        #       gRC - Regional Character Table
        #       gCC - Country Character Table -
        gRC = economic_research.get_gRC_region_character()
        gCC = economic_research.get_gCC_country_character()
        gResearchCxI = economic_research.get_gResearchCxI()

        ############################################################################################################################
        #     Country Model Forecast Generation
        #
        market_ww_forecast_x_region = market_report_data.get_market_forecast_X_region_table(gRegionList)
        market_ww_forecast_x_industry = market_report_data.get_market_forecast_X_industry_table(gIndustryList)

        country_model_forecast = Country_Model_Forecast ( market_ww_forecast_x_region, market_ww_forecast_x_industry,self.baseYear,gRC, gCC, gRegionList, gIndustryList)
        _sRxIp_country_model_forecast = country_model_forecast.get_sRxIp_Table()
        country_known_forecast = Report_CountryKnowns_Market_Forecast(self.db_cxcn, self.marketStudy, self.baseYear, economic_research)
        country_known_forecast_table = country_known_forecast.get_CountryKnown_Table()

        _ukRxI_country_model_forecast = country_known_forecast.get_ukRxI(market_ww_forecast_x_region,_sRxIp_country_model_forecast, gRegion_x_Country)
        # CMF-Step-3
        _ukCpRxI_country_model_forecast = country_known_forecast.get_ukCpRxI(gResearchCxI)
        # CMF-Step-5
        #   Compute the Country by Industry revenue matrix for unknown countries
        _ukCxI = country_known_forecast.get_ukCxI(_ukRxI_country_model_forecast , _ukCpRxI_country_model_forecast)

        # CMF-Step-6
        #    '** Compute Country by Industry for country known values
        #    '**   Compute an industry distribution to use when only the country total is known
        _kCxIp = country_known_forecast.get_kCxIp(market_ww_forecast_x_industry, gCC)

        # CMF-Step-7
        _kCxI = country_known_forecast.get_kCxI (_kCxIp)

        # CMF-Step-8
        # **   Combine Known Country Values with Computed Unknown Estimates
        country_model_forecast = country_known_forecast.get_Merge_Knowns(_ukCxI, _kCxI)
        country_model_forecast['Study'] = self.marketStudy
        return country_model_forecast

    def __init__(self, cxcn,  market_study, year):
        self.marketStudy = market_study
        self.baseYear = year
        self.db_cxcn = cxcn



