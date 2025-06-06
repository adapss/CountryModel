from email.policy import default
from itertools import groupby
import sys
import pyodbc
import pandas as pd
import numpy as np
import warnings
from sqlalchemy import create_engine
from sqlalchemy import text

from myCountryModelPackages.sqlTableRetrieve import *
from myCountryModelPackages.MarketReportRetrieval import *
from myCountryModelPackages.CountryModel_Forecast import *
from myCountryModelPackages.CountryModel_MarketSize import *
from myCountryModelPackages.Economic_Research import *

class CountryModelComparisonTest:
    market_share_generated = None
    market_share_vba = None
    market_forecast_generated = None
    market_forecast_vba = None

    def market_share_comparison(self):
        market_share_comparison  = self.market_share_generated.merge(self.market_share_vba, on=['BaseYear', 'Study','Company','Region','Segment','Industry'],how='left')
        market_share_comparison['Diff'] = abs(market_share_comparison['Size_x'] - market_share_comparison['Size_y'])
        return market_share_comparison

    def market_forecast_comparison(self):
        market_forecast_comparison  = self.market_forecast_generated.merge(self.market_forecast_vba, on=['BaseYear','Year', 'Study','Region','Segment','Country','Industry'],how='left')
        market_forecast_comparison['Diff'] = abs(market_forecast_comparison['Forecast_x'] - market_forecast_comparison['Forecast_y'])
        return market_forecast_comparison

    def __init__(self, market_share_generated, market_share_vba, forecast_generated, forecasted_vba ):
        self.market_share_generated = market_share_generated
        self.market_share_vba = market_share_vba
        self.market_forecast_generated = forecast_generated
        self.market_forecast_vba = forecasted_vba


# Country_Model_Publish  - used to push a generated country model to the database
#  Instantiate the class with references to the to
#       db_engine - connection to the database
#       report - report name
#       base_year
#       Market Share dataframe
#       Forecast dataframe
# Then you have to call the "publish" methods.
class Country_Model_Publish:
    db_engine_market_data = None
    market_report = None
    base_year = None
    market_shares = None
    market_forecast = None

    def publish_market_shares(self):
        _sql_statement = """
            DELETE FROM [dbo].[StudySizesCountryModel]
            WHERE [Study] = :study AND [BaseYear] = :base_year
            """
        try:
            with self.db_engine_market_data.connect() as connection:
                result = connection.execute( text(_sql_statement), {"study": self.market_report, "base_year": self.base_year})
                connection.commit()
        except Exception as e:
            print(f"Error executing SQL statement: {e}")

        _market_shares = self.market_shares.rename(columns={'Industry': 'ParentCategory','Region':'Category'})
        _market_shares['GrandParentCategory']= _market_shares['GrandParentCategory']=0
        _market_shares['Units']="Revenues"
        #_market_shares['SizeKey']=_market_shares['BaseYear']+"~"+_market_shares['Study']+"~"+_market_shares['Company']+"~"+_market_shares['Segment']+"~"+_market_shares['Category']+"~"+_market_shares['ParentCategory']+"~"+_market_shares['GrandParentCategory']
        _market_shares['SizeKey'] = (
                _market_shares['BaseYear'].astype(str) + "~" +
                _market_shares['Study'].astype(str) + "~" +
                _market_shares['Company'].astype(str) + "~" +
                _market_shares['Segment'].astype(str) + "~" +
                _market_shares['Category'].astype(str) + "~" +
                _market_shares['ParentCategory'].astype(str) + "~" +
                _market_shares['GrandParentCategory'].astype(str)
                )

        _market_shares.to_sql('StudySizesCountryModel', self.db_engine_market_data, if_exists='append', index=False)
        return

    def publish_market_forecast(self):
        sql_statement = """
            DELETE FROM [dbo].[StudyForecastsCountryModel]
            WHERE ([Study] = :study) AND ([BaseYear] = :base_year)
            """
        try:
            with self.db_engine_market_data.connect() as connection:
                result = connection.execute(text(sql_statement), {"study": self.market_report, "base_year": self.base_year})
                connection.commit()
        except Exception as e:
            print(f"Error executing SQL statement: {e}")

        _market_forecast = self.market_forecast.rename(columns={'Industry': 'GrandParentCategory', 'Region': 'Category', 'Country': 'ParentCategory'})
        _market_forecast['Units']="Revenues"
        _market_forecast[('ForecastKey')] = (
                _market_forecast['BaseYear'].astype(str) + "~" +
                _market_forecast['Study'].astype(str) + "~" +
                _market_forecast['Segment'].astype(str) + "~" +
                _market_forecast['Category'].astype(str) + "~" +
                _market_forecast['ParentCategory'].astype(str) + "~" +
                _market_forecast['GrandParentCategory'].astype(str)
                )
        _market_forecast.to_sql('StudyForecastsCountryModel', self.db_engine_market_data, if_exists='append', index=False)
        return

    def __init__(self, db_engine, report, base_year, market_shares, market_forecast):
        self.market_report = report
        self.base_year = base_year
        self.market_shares = market_shares
        self.market_forecast = market_forecast
        self.db_engine_market_data = db_engine


class Country_Model_Generation:
    marketStudy = "AC Drives Low Voltage"
    baseYear:str = "2023"
    dump = False
    db_cxcn = None
    # db_cxcn_economic_research = None

    def generate_market_shares(self):
        # Economic Tables from Analyst Research
        #  - build Lists Countries, Industries and Regions included in the Economic analysis
        economic_research = CountryEconomicResearch(self.baseYear)
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
        country_model_market_share['Size_y']  = country_model_market_share['Size_y'] .fillna(0)
        country_model_market_share['Size'] = (country_model_market_share  ['Size_x'] + country_model_market_share ['Size_y'])
        country_model_market_share = country_model_market_share.drop(['Size_x','Size_y'],axis=1)
        country_model_market_share['Study']= self.marketStudy
        country_model_market_share['BaseYear']= self.baseYear
        country_model_market_share['Segment'] = "World Region"
        return country_model_market_share

    def generate_forecast(self):
       # Economic Tables from Analyst Research
        #  - build Lists Countries, Industries and Regions included in the Economic analysis
        economic_research = CountryEconomicResearch(self.baseYear)
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
        country_model_forecast['BaseYear'] = self.baseYear
        return country_model_forecast

    def __init__(self, cxcn, market_study, year):
        self.marketStudy = market_study
        self.baseYear = year
        self.db_cxcn = cxcn




