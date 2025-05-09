import sys
import warnings
import pyodbc
import pandas as pd
import numpy as np

from myCountryModelPackages.Economic_Research import *

class Report_Market_Size_Data:
    market_size_data = None
    market_forecast_data = None
    connection = None
    sql_query_market_size = None
    sql_query_market_forecast = None
    year = None

    def get_MarketSize_Table(self):
        return self.market_size_data

    # 3) Build study-Region_X_Industry table.
    #   - derived from Economic data and market study data
    #   - Economic research provides granularity down to the regional industries.
    def get_sRxIp_Table(self, gRC):
        industry_list = gRC['Industry'].drop_duplicates()
        company_X_industry = self.get_MarketSize_X_Industry_Table(industry_list)
        _temp = company_X_industry.groupby(['Company', 'Industry'])['Size'].sum().reset_index()
        _temp2 = _temp.merge(gRC, on='Industry', how='left')
        _temp2['Size'] = _temp2['Size_x'] * _temp2['Size_y']
        _temp3 = _temp2.groupby(['Company', 'Region'])['Size'].sum().reset_index()
        _temp3 = _temp3.rename(columns={'Size': 'TotalRegionSize'})
        _temp4 = _temp2.merge(_temp3, on=['Company', 'Region'], how='left')
        _temp4['SizeP'] = _temp4['Size'] / _temp4['TotalRegionSize']
        _temp4 = _temp4.drop(['Size_x','Size_y','TotalRegionSize','Size'], axis=1)
        _temp4 = _temp4.rename(columns={'SizeP': 'Size'})
        return _temp4

    def get_CompanyList(self):
        company_list = self.market_size_data['Company'].drop_duplicates()
        company_list = company_list[company_list.isin(['ABB','Hitachi'])]
        return company_list

    def get_Region_X_Industry_Table(self,companyList, industryList,regionList ):
        multi_index = pd.MultiIndex.from_product([companyList, regionList, industryList], names=['Company','Region', 'Industry'])
        # Create DataFrame with the MultiIndex
        cXrXI_combinations = pd.DataFrame(index=multi_index)
        # Reset index to bring the combined values into columns
        cXrXI = cXrXI_combinations.reset_index()
       # merged_df = pd.DataFrame(companyList, industryList, regionList)
        cXrXI['Size'] = 0
        return cXrXI

    def get_MarketSize_X_Industry_Table(self, industry_list):
        productByCompanyByIndustryByYr = self.market_size_data.loc[(self.market_size_data['Segment'] == "Industry") & (self.market_size_data['Category'].isin(industry_list)), ['Company', 'Segment', 'Category', 'Size']]
        productByCompanyByIndustryByYr  = productByCompanyByIndustryByYr .rename(columns={'Category': 'Industry'})
        return productByCompanyByIndustryByYr

    def get_MarketSize_X_Region_Table(self,RegionList ):
        productByCompanyByRegionByYr = self.market_size_data.loc[(self.market_size_data['Segment'] == "World Region") & (self.market_size_data['Category'].isin(RegionList)), ['Company', 'Segment', 'Category', 'Size']]
        productByCompanyByRegionByYr = productByCompanyByRegionByYr.rename(columns={'Category': 'Region'})
        return productByCompanyByRegionByYr

    def get_market_forecast_X_region_table(self,RegionList ):
        region_x_year = self.market_forecast_data.loc[(self.market_forecast_data['Segment'] == "World Region") & (self.market_forecast_data['Category'].isin(RegionList)), ['Segment', 'Category', 'Forecast','Year']]
        region_x_year = region_x_year.rename(columns={'Category': 'Region'})
        return region_x_year

    def get_market_forecast_X_industry_table(self,industry_list ):
        industry_x_year = self.market_forecast_data.loc[(self.market_forecast_data['Segment'] == "Industry") & (self.market_forecast_data['Category'].isin(industry_list)), ['Segment', 'Category', 'Forecast','Year']]
        industry_x_year = industry_x_year.rename(columns={'Category': 'Industry'})
        return industry_x_year

    def __init__(self,cxcn, marketStudy,year):
        worldSegment = "World Region"
        industrySegment = "Industry"
        categoryName = "%"
        self.connection = cxcn
        self.year = year
        self.sql_query_market_size = \
            f"SELECT  [SizeKey], [Study], [BaseYear], [Company], [Segment], [Category],[ParentCategory], [Size], [Fraction], [Units] FROM [dbo].[StudySizes]" \
            f"WHERE   ([Study] = '{marketStudy}') AND ([BaseYear] =  {year}) AND (([Segment] LIKE '{worldSegment}')  OR ([Segment] LIKE '{industrySegment}'))" \
            f"AND ([Category] LIKE '{categoryName}')" \
            f"ORDER BY [Category], [BaseYear], [Company]"
        self.market_size_data = pd.read_sql(self.sql_query_market_size, self.connection)
        self.sql_query_market_forecast = \
            f"SELECT  [ForecastKey], [Study], [BaseYear], [Year],[Segment], [Category],[ParentCategory], [Forecast], [Units] FROM [dbo].[StudyForecasts]" \
            f"WHERE   ([Study] = '{marketStudy}') AND ([BaseYear] =  {year}) AND (([Segment] LIKE '{worldSegment}')  OR ([Segment] LIKE '{industrySegment}'))" \
            f"AND ([Category] LIKE '{categoryName}')" \
            f"ORDER BY [Segment], [Category], [BaseYear], [Year] "
        self.market_forecast_data = pd.read_sql(self.sql_query_market_forecast, self.connection)



class Report_CountryKnowns_Market_Size:
    country_known_size = None
    connection = None
    queryCountryKnown = None
    year = None

    def get_CountryKnown_Table(self):
        return self.country_known_size

    # Step 4-b Country Known Inclusion
    #  - Compute the Region totals by company derived from the Country Known Table.
    #  - build a table of
    def get_ukRxI(self,company_X_region, sRxIp):
        _kRegion_Total = self.country_known_size.groupby(['Company', 'Region'])['Size'].sum().reset_index()
        _company_X_region = company_X_region.groupby(['Company', 'Region'])['Size'].sum().reset_index()
        _merge_study_knowns = _company_X_region.merge(_kRegion_Total, on=['Company', 'Region'], how='left')
        _merge_sRxI = _merge_study_knowns.merge(sRxIp, on=['Company', 'Region'], how='left')
        _merge_sRxI['kRemove_Size'] = (_merge_sRxI['Size_x'] - _merge_sRxI['Size_y']) * _merge_sRxI['Size']
        _merge_sRxI = _merge_sRxI.drop(['Size_x', 'Size_y','Size'], axis=1)
        _merge_sRxI.rename(columns={'kRemove_Size': 'Size'}, inplace=True)
        return _merge_sRxI

    def get_kRxI(self, kCxIp):
        _kRxI = self.country_known_size.loc[(self.country_known_size['Industry'] == "Other Industries"), ['Region', 'Country', 'Company', 'Industry', 'Size']]
        _kRxI = pd.merge(_kRxI, kCxIp, on=['Company','Region', 'Country'])
        _kRxI['Size'] = _kRxI['Size_x'] * _kRxI['Size_y']
        _kRxI = _kRxI.drop(['Size_x', 'Size_y', 'Industry_x'], axis=1)
        _kRxI.rename(columns={'Industry_y': 'Industry'}, inplace=True)
        kRxI = _kRxI.groupby(['Company', 'Region','Industry'])['Size'].sum().reset_index()
        return kRxI

    def get_kCxIp(self, company_X_industry, gCC, economic_research):
        valid_country_known = self.get_country_known_list(economic_research)
        valid_Industry_list = economic_research.get_IndustryList()
        _gCC = gCC.loc[gCC['Country'].isin(valid_country_known)]
        indRevForCountry_totals =  company_X_industry.groupby(['Company', 'Industry'])[
            'Size'].sum().reset_index()
        indRevForCountry = pd.merge(_gCC,indRevForCountry_totals,on=['Industry'])
        indRevForCountry['Size'] = indRevForCountry['Size_x']*indRevForCountry['Size_y']
        indRevForCountry =   indRevForCountry.drop(['Size_x', 'Size_y'], axis=1)
        indRev_country_total = indRevForCountry.groupby(['Company','Country'])['Size'].sum().reset_index()
        new_kCxIp = pd.merge(indRevForCountry,indRev_country_total,on=['Company','Country'])
        new_kCxIp['Size'] = np.where(new_kCxIp['Size_y'] == 0, 0, new_kCxIp['Size_x'] / new_kCxIp['Size_y'])
        new_kCxIp = new_kCxIp.drop(['Size_x','Size_y'], axis=1)
        return new_kCxIp

    def get_country_known_list(self,economic_research):
        countries = economic_research.get_CountryList()
        return self.country_known_size.loc[self.country_known_size['Country'].isin(countries), 'Country'].drop_duplicates()

    def __init__(self,cxcn, marketStudy,year,economic_research):
        gRegionList = economic_research.get_RegionList()
        gRegion_x_Country = economic_research.get_Region_X_Country_Table()
        self.connection = cxcn
        self.year = year
        self.queryCountryKnown = f"SELECT [Study], [BaseYear],[Country],[Industry],[Company], [Size] FROM [dbo].[CountrySizes]" \
                            f" WHERE   (([Study] = '{marketStudy}') AND ([BaseYear] =  {year}))" \
                            f" ORDER BY [Study], [BaseYear], [Company], [Country], [Industry]"
        self.country_known_size = pd.read_sql(self.queryCountryKnown, self.connection)
        self.country_known_size = self.country_known_size.merge(gRegion_x_Country, on='Country', how='left')
        self.country_known_size = self.country_known_size[self.country_known_size['Region'].isin(gRegionList)]

