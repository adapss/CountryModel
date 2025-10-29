import pandas as pd
from sqlalchemy import text
from app.myCountryModelPackages.sqlTableRetrieve import DatabaseConnections

class CountryEconomicResearch:
    economic_data = None
    economic_data_vba = None
   # connection = None
    year = None

    def get_Economic_Comparison(self):
        economic_research_compare = None
        self.economic_data_vba = self.get_VBA_Generated_EconomicTable()
        economic_research_compare = self.economic_data.merge(self.economic_data_vba,on = ['BaseYear', 'Region','Country','Industry'], how='left')
        economic_research_compare['Delta'] = abs(economic_research_compare['CountryWeight_x'] - economic_research_compare['CountryWeight_y'])
        return economic_research_compare

    def get_VBA_Generated_EconomicTable(self):
        db_cxcn_economic_research = DatabaseConnections().get_MiraLite_Connection()
        query_economic_research = \
            f"SELECT [BaseYear],[Country],[Region],[Industry],[AutomationDegree],[IndustryFraction], [CountryWeight],[RangeDate] FROM [dbo].[CountryEconomicData]" \
            f" WHERE [SetName] = '" + "Economic Model" + "' " \
            f" AND [BaseYear]  = '{self.year}' "
        self.economic_data_vba = pd.read_sql(query_economic_research, db_cxcn_economic_research)
        self.economic_data_vba = self.economic_data_vba.drop(['RangeDate', 'AutomationDegree'], axis=1)
        return self.economic_data_vba

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

    #def getConnection(self):
     #   print(self.myConnection)

    def __init__(self,year):
        # self.connection = cxcn
        self.year = year
        self.economic_data = Economic_Research_Create(self.year).get_EconomicResearch()

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
        self.connection = DatabaseConnections().get_MiraIndustry_Connection()
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
        self.country_gdp_annual['Country'] = self.country_gdp_annual['Country'].str.strip()

# The Economic Research is generated dynamically in this class.
#  Required are two tables:
#       CountryModel_AutomationDegree
#           IndustryFraction = for each country list of industries and what weight is in the country
#           Automation Degree  = what degree of automation in that industry/country combination
#       CountryModel_IndustryGDP
#           IndustrialGDP_Fraction  =  GDP by country is a fractional multiplier of the total country GDP
class Economic_Research_Create:
    year = None
    country_model_industry_gdp_fraction = None
    country_model_industry_automation_degree = None
    economic_research_data = None
    gdp_x_country = None

    def get_gdp_x_country(self):
        return self.gdp_x_country

    def get_EconomicResearch(self):
        return self.economic_research_data

    def get_industrial_gdp_fraction(self):
        return self.country_model_industry_gdp_fraction

    def get_industrial_automation_degree(self):
        return self.country_model_industry_automation_degree

    def __init__(self, year):
        self.connection = DatabaseConnections().get_MiraIndustry_Connection()
        self.year = year
        sql_statement_industry_fractions = \
            f"SELECT [BaseYear],[Region], [Country],[Industry], [IndustryFraction] FROM [dbo].[CountryModel_IndustryFraction] " \
            f"WHERE [BaseYear] = '{self.year}' " \
            f"ORDER BY [Region], [Country] "
        self.country_model_industry_fraction = pd.read_sql(sql_statement_industry_fractions, self.connection)
        self.country_model_industry_fraction['Region'] = self.country_model_industry_fraction['Region'].str.strip()
        self.country_model_industry_fraction['Country'] = self.country_model_industry_fraction['Country'].str.strip()
        self.country_model_industry_fraction['Industry'] = self.country_model_industry_fraction['Industry'].str.strip()

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
            f"SELECT [BaseYear],[Region], [Country],[Industry],[AutomationDegree] FROM [dbo].[CountryModel_AutomationDegree] " \
            f"WHERE [BaseYear] = '{self.year}' " \
            f"ORDER BY [Region], [Country] "
        self.country_model_industry_automation_degree = pd.read_sql(sql_statement_automation_degree, self.connection)
        self.country_model_industry_automation_degree['Region'] = self.country_model_industry_automation_degree['Region'].str.strip()
        self.country_model_industry_automation_degree['Country'] = self.country_model_industry_automation_degree['Country'].str.strip()
        self.country_model_industry_automation_degree['Industry'] = self.country_model_industry_automation_degree['Industry'].str.strip()

        self.economic_research_data = self.gdp_x_country.merge(self.country_model_industry_gdp_fraction,on = ['BaseYear', 'Region','Country'], how='left')
        self.economic_research_data ['IndustrialGDP_Fraction'] = self.economic_research_data ['IndustrialGDP_Fraction'].fillna(1)
        self.economic_research_data = self.economic_research_data.merge(self.country_model_industry_automation_degree,on = ['BaseYear', 'Region','Country'], how='left')
        self.economic_research_data = self.economic_research_data.merge(self.country_model_industry_fraction,on = ['BaseYear', 'Region','Country','Industry'], how='left')

        self.economic_research_data['CountryWeight'] = self.economic_research_data['GDP'] * self.economic_research_data['IndustrialGDP_Fraction'] * self.economic_research_data['IndustryFraction']* self.economic_research_data['AutomationDegree']

        self.economic_research_data = self.economic_research_data.drop(['GDP', 'IndustrialGDP_Fraction', 'AutomationDegree'], axis=1)

# EconomicResearchFactorsRanges
# Specifically to retrieve the ranges in the IndustryWeight table
#
class EconomicResearchFactorsRanges:
    connection = None

    def __init__(self):
        db_connection = DatabaseConnections()
        self.connection = db_connection.get_MiraIndustry_Connection()

    def __get_gdp_regional_remainder_table(self):
        year = "%"
        sql_statement_gdp_remainders= \
            f"SELECT [Year],[Region],[Country],[RemainderSize] FROM [dbo].[FinancialMetrics_GDP_Region_Remainder] " \
            f"WHERE [Year] LIKE '{year}' " \
            f"ORDER BY [Region] "
        _gdp_remainders = pd.read_sql(sql_statement_gdp_remainders, self.connection)
        return  _gdp_remainders

    def __get_automation_gdp_table(self):
        year = "%"
        sql_statement_automation_gdp = \
            f"SELECT [BaseYear],[Region], [Country],[IndustrialGDP_Fraction] FROM [dbo].[CountryModel_IndustryGDP] " \
            f"WHERE [BaseYear] LIKE '{year}' " \
            f"ORDER BY [Region], [Country] "
        _industry_gdp = pd.read_sql(sql_statement_automation_gdp, self.connection)
        return  _industry_gdp

    def __get_automation_degree_table(self):
        year = "%"
        sql_statement_automation_degree = \
            f"SELECT [BaseYear],[Region], [Country],[Industry],[AutomationDegree] FROM [dbo].[CountryModel_AutomationDegree] " \
            f"WHERE [BaseYear] LIKE '{year}' " \
            f"ORDER BY [Region], [Country] "
        _industry_automation_degree = pd.read_sql(sql_statement_automation_degree, self.connection)
        return _industry_automation_degree

    def __get_industry_weight_table(self):
        year = "%"
        sql_statement_industry_weights = \
            f"SELECT [BaseYear],[Region], [Country],[Industry],[IndustryFraction] FROM [dbo].[CountryModel_IndustryFraction] " \
            f"WHERE [BaseYear] LIKE '{year}' " \
            f"ORDER BY [Region], [Country] "
        _industry_fractions = pd.read_sql(sql_statement_industry_weights, self.connection)
        return _industry_fractions

    #******* get_economic_research_years
    # There are 4 tables which need to be validated that they are all populated with the same range in years.
    #   - gdp data by country
    #   - automation degree
    #   - industry Weights
    #   - gdp remainders
    #  the intersection of the years of the Economic research will determine what years of market reports can be modeled.
    def get_economic_research_years(self):
        gdp_years = self.get_gdp_research_years()
        automation_degree_years = self.get_industry_weights_research_years()
        industry_years = self.get_industry_weights_research_years()
        gdp_remainders_years = self.get_gdp_regional_remainder_research_years()
        intersection_years = set(gdp_years) & set(industry_years) & set(gdp_remainders_years)
        return list(intersection_years)

    def get_gdp_regional_remainder_research_years(self):
        _industry_gdp = self.__get_gdp_regional_remainder_table()
        _industry_gdp = _industry_gdp.drop(['Region','Country','RemainderSize'],axis=1)
        base_year_list = _industry_gdp['Year'].drop_duplicates()
        base_year_list = base_year_list.sort_values(ascending=False)
        return base_year_list.to_list()

    def get_gdp_research_years(self):
        _industry_gdp = self.__get_automation_gdp_table()
        _industry_gdp = _industry_gdp.drop(['Region','Country','IndustrialGDP_Fraction'],axis=1)
        base_year_list = _industry_gdp['BaseYear'].drop_duplicates()
        base_year_list = base_year_list.sort_values(ascending=False)
        return base_year_list.to_list()

    def get_regions_from_regional_remainder(self, year):
        _gdp_remainders= self.__get_gdp_regional_remainder_table()
        _gdp_remainders = _gdp_remainders[(_gdp_remainders['Year'] == year)]
        _gdp_remainders = _gdp_remainders.drop(['Year','Country','RemainderSize'],axis=1)
        region_list = _gdp_remainders['Region'].drop_duplicates()
        region_list = region_list.sort_values(ascending=False)
        return region_list

    def get_gdp_research_regions(self, year):
        _industry_automation_gdp = self.__get_automation_gdp_table()
        _industry_automation_gdp = _industry_automation_gdp[(_industry_automation_gdp['BaseYear'] == year)]
        _industry_automation_gdp = _industry_automation_gdp.drop(['BaseYear','Country','IndustrialGDP_Fraction'],axis=1)
        region_list = _industry_automation_gdp['Region'].drop_duplicates()
        region_list = region_list.sort_values(ascending=False)
        return region_list

    def get_gdp_research_countries(self, year, region):
        _industry_automation_gdp = self.__get_automation_gdp_table()
        _industry_automation_gdp = _industry_automation_gdp[(_industry_automation_gdp['BaseYear']==year) & (_industry_automation_gdp['Region']==region)]
        _industry_automation_gdp = _industry_automation_gdp.drop(['BaseYear','Region','IndustrialGDP_Fraction'],axis=1)
        country_list = _industry_automation_gdp['Country'].drop_duplicates()
        country_list = country_list.sort_values(ascending=False)
        return country_list

    def get_industry_weights_research_years(self):
        _industry_weights = self.__get_industry_weight_table()
        _industry_weights = _industry_weights.drop(['Region','Country','Industry','IndustryFraction'],axis=1)
        base_year_list = _industry_weights['BaseYear'].drop_duplicates()
        base_year_list = base_year_list.sort_values(ascending=False)
        return base_year_list.to_list()

    def get_industry_weights_research_regions(self, year):
        _industry_weights = self.__get_industry_weight_table()
        _industry_weights =  _industry_weights[( _industry_weights['BaseYear'] == year)]
        _industry_weights =  _industry_weights.drop(['BaseYear','Country','Industry','IndustryFraction'],axis=1)
        region_list =  _industry_weights['Region'].drop_duplicates()
        region_list = region_list.sort_values(ascending=False)
        return region_list.to_list()

    def get_countries_from_industry_weights_research(self, year, region):
        _industry_weights = self.__get_industry_weight_table()
        _industry_weights = _industry_weights[(_industry_weights['BaseYear']==year) & (_industry_weights['Region']==region)]
        _industry_weights = _industry_weights.drop(['BaseYear','Region','Industry','IndustryFraction'],axis=1)
        country_list = _industry_weights['Country'].drop_duplicates()
        country_list = country_list.sort_values(ascending=False)
        return country_list



# EconomicResearchFactorsPublish - support write of a Economic Factors dataframes to the database
# These are the methods available:
#   publish_industry_weights
#   publish_country_gdp_fraction
#   publish_regional_gdp_remainders
#
class EconomicResearchFactorsPublish():
    db_engine_economic_research = None
    db_connection = None

    # Comprehensive Copy function for both Technology Group and Universal modeling parameters.
    # Either create a new year model or simply copy over existing modeling data
    # All 4 tables will be copied and either write over existing tables or create new entries.
    def copy_economic_research(self, copy_year:int, new_year:int ):
        # Automation Degree by Country X Industry
        sql_statement_automation_degree = f"""
            SELECT [BaseYear], [Region], [Country], [Industry], [AutomationDegree], [TechnologyGroupID]
            FROM [dbo].[CountryModel_AutomationDegree]
            WHERE [BaseYear] = {copy_year} 
            ORDER BY [BaseYear],[Region], [Country]
        """
        automation_degree = pd.read_sql(sql_statement_automation_degree, self.db_connection)
        automation_degree['BaseYear'] = new_year

        delete_query = f"""
            DELETE FROM CountryModel_AutomationDegree
            WHERE BaseYear = {new_year}
        """
        cursor = self.db_connection.cursor()
        cursor.execute(delete_query)
        self.db_connection.commit()

        automation_degree.to_sql('CountryModel_AutomationDegree', self.db_engine_economic_research, if_exists='append',
                                 index=False)

        # Industrial GDP Table
        sql_statement_industry_gdp = f"""
        SELECT [BaseYear], [Region], [Country], [IndustrialGDP_Fraction], [TechnologyGroupID]
        FROM [dbo].[CountryModel_IndustryGDP]
        WHERE [BaseYear] = {copy_year} 
        ORDER BY [BaseYear], [Region], [Country]
        """
        industry_gdp = pd.read_sql(sql_statement_industry_gdp, self.db_connection)
        industry_gdp['BaseYear'] = new_year

        delete_query = f"""
                 DELETE FROM CountryModel_IndustryGDP
                 WHERE BaseYear = {new_year}
            """
        cursor = self.db_connection.cursor()
        cursor.execute(delete_query)
        self.db_connection.commit()
        industry_gdp.to_sql('CountryModel_IndustryGDP', self.db_engine_economic_research, if_exists='append',
                                  index=False)

        # Industry Weights x Country Table
        sql_statement_industry_weights = f"""
                SELECT [BaseYear], [Region], [Country], [Industry], [IndustryFraction]
                FROM [dbo].[CountryModel_IndustryFraction]
                WHERE [BaseYear] = {copy_year} 
                ORDER BY [BaseYear], [Region], [Country], [Industry]
                """
        industry_weights = pd.read_sql(sql_statement_industry_weights, self.db_connection)
        industry_weights['BaseYear'] = new_year

        delete_query = f"""
                         DELETE FROM CountryModel_IndustryFraction
                         WHERE BaseYear = {new_year}
                    """
        cursor = self.db_connection.cursor()
        cursor.execute(delete_query)
        self.db_connection.commit()
        industry_weights.to_sql('CountryModel_IndustryFraction', self.db_engine_economic_research, if_exists='append',
                            index=False)


        # GDP Remainders by Region
        sql_statement_gdp_remainders = f"""
                SELECT [Year], [Region], [Country], [RemainderSize]
                FROM [dbo].[FinancialMetrics_GDP_Region_Remainder]
                WHERE [Year] = {copy_year} 
                ORDER BY [Year], [Region], [Country]
                """
        gdp_remainders = pd.read_sql(sql_statement_gdp_remainders, self.db_connection)
        gdp_remainders['Year'] = new_year
        delete_query = f"""
                         DELETE FROM FinancialMetrics_GDP_Region_Remainder
                         WHERE Year = {new_year}
                    """
        cursor = self.db_connection.cursor()
        cursor.execute(delete_query)
        self.db_connection.commit()
        gdp_remainders.to_sql('FinancialMetrics_GDP_Region_Remainder', self.db_engine_economic_research, if_exists='append',
                            index=False)

        return "Copy Complete"

    # Archive -(TGF = Technology Group Factors) by Version Key
    def archive_techgroup_economic_research(self, version:int, first_year:int, last_year:int, version_comment:str):
        sql_statement_cm_versions = \
            f"SELECT [Version],[Comment], [Modified],[VersionKey] FROM [dbo].[CMV_TGF_VersionManagement] " \
            f"ORDER BY [Version],[Modified] "
        version_table = pd.read_sql(sql_statement_cm_versions, self.db_connection)

        if version in version_table['Version'].values:
            raise ValueError(f"Version {version} already exists in CMV_VersionManagement.")

        new_version_table_row = pd.DataFrame({
            'Version': [version],
            'YearFirst': [first_year],
            'YearLast': [last_year],
            'Comment': [version_comment],
            'Modified': [pd.Timestamp.now()]
        })

        new_version_table_row.to_sql('CMV_TGF_VersionManagement', self.db_engine_economic_research, if_exists='append',
                             index=False)

        # fetches the VersionKey of the most recently modified record in the CMV_TGF_VersionManagement table
        query = """
        SELECT TOP 1 VersionKey
        FROM [dbo].[CMV_TGF_VersionManagement]
        WHERE Version = ? AND Comment = ?
        ORDER BY Modified DESC
        """
        cursor = self.db_connection.cursor()
        cursor.execute(query, (int(version), str(version_comment)))
        version_key = cursor.fetchone()[0]

        # Automation Degree by Country X Industry Archiving
        sql_statement_automation_degree = f"""
            SELECT [BaseYear], [Region], [Country], [Industry], [AutomationDegree], [TechnologyGroupID]
            FROM [dbo].[CountryModel_AutomationDegree]
            WHERE [BaseYear] BETWEEN {first_year} AND {last_year}
            ORDER BY [Region], [Country]
        """
        automation_degree = pd.read_sql(sql_statement_automation_degree, self.db_connection)
        automation_degree['VersionKey'] = version_key
        automation_degree.to_sql('CMV_TGF_AutomationDegree', self.db_engine_economic_research, if_exists='append',
                                 index=False)

        # Industrial GDP Multiplier archiving
        sql_statement_industry_GDP_fraction = f"""
            SELECT [BaseYear], [Region], [Country], [IndustrialGDP_Fraction], [TechnologyGroupID]
            FROM [dbo].[CountryModel_IndustryGDP]
            WHERE [BaseYear] BETWEEN {first_year} AND {last_year}
            ORDER BY [Region], [Country]
        """

        industry_gdp = pd.read_sql(sql_statement_industry_GDP_fraction, self.db_connection)
        industry_gdp['VersionKey'] = version_key
        industry_gdp.to_sql('CMV_TGF_IndustryGDP', self.db_engine_economic_research, if_exists='append',
                                  index=False)
        return "Version Created"

    # Archive the Universal Economic Factors which include the IndustryFraction and GDP Remainder tables
    def archive_uef_economic_research(self, version:int, first_year:int, last_year:int, version_comment:str):
        sql_statement_cm_versions = \
            f"SELECT [Version],[Comment], [Modified],[VersionKey] FROM [dbo].[CMV_UEF_VersionManagement] " \
            f"ORDER BY [Version],[Modified] "
        version_table = pd.read_sql(sql_statement_cm_versions, self.db_connection)

        if version in version_table['Version'].values:
            raise ValueError(f"Version {version} already exists in CMV_UEF_VersionManagement.")

        new_version_table_row = pd.DataFrame({
            'Version': [version],
            'YearFirst':[first_year],
            'YearLast': [last_year],
            'Comment': [version_comment],
            'Modified': [pd.Timestamp.now()]
        })

        new_version_table_row.to_sql('CMV_UEF_VersionManagement', self.db_engine_economic_research, if_exists='append',
                             index=False)
        # fetches the VersionKey of the most recently modified record in the CMV_UEF_VersionManagement table
        query = """
        SELECT TOP 1 VersionKey
        FROM [dbo].[CMV_UEF_VersionManagement]
        WHERE Version = ? AND Comment = ?
        ORDER BY Modified DESC
        """
        cursor = self.db_connection.cursor()
        cursor.execute(query, (int(version), str(version_comment)))
        version_key = cursor.fetchone()[0]

        # Industry Fraction by Country X Industry Archiving
        sql_statement_industry_fraction = \
            f"SELECT [BaseYear],[Region], [Country],[Industry],[IndustryFraction] FROM [dbo].[CountryModel_IndustryFraction] " \
            f"WHERE[BaseYear] BETWEEN {first_year} AND {last_year}" \
            f"ORDER BY [Region], [Country] "

        industry_fractions = pd.read_sql(sql_statement_industry_fraction, self.db_connection)
        industry_fractions['VersionKey'] = version_key
        industry_fractions.to_sql('CMV_UEF_IndustryFraction', self.db_engine_economic_research, if_exists='append',
                                 index=False)

        # GDP remainders by region, for those regions where we don't have all the countries.
        sql_statement_GDP_rem = \
            f"SELECT [Year],[Region], [Country],[RemainderSize] FROM [dbo].[FinancialMetrics_GDP_Region_Remainder] " \
            f"WHERE[Year] BETWEEN {first_year} AND {last_year}" \
            f"ORDER BY [Region], [Country] "
        gdp_rem = pd.read_sql(sql_statement_GDP_rem, self.db_connection)
        gdp_rem['VersionKey'] = version_key
        gdp_rem.to_sql('CMV_UEF_GDP_Remainder', self.db_engine_economic_research, if_exists='append',
                                  index=False)
        return "Version Created"

    # Restore -(Universal Economic Factors) by Version Key
    def restore_version_uef_economic_research(self, versionKey:int, first_year:int, last_year:int ):

        # Country Model Version - Industry Fractions retrieving partition to be restored.
        # Industry Fraction by Country X Industry Archiving
        sql_statement_industry_fraction = \
            f"SELECT [BaseYear],[Region], [Country],[Industry],[IndustryFraction] FROM [dbo].[CMV_UEF_IndustryFraction] " \
            f"WHERE [VersionKey] = '{versionKey}' " \
            f"ORDER BY [Region], [Country] "
        industry_fractions = pd.read_sql(sql_statement_industry_fraction, self.db_connection)

        delete_query = f"""
            DELETE FROM CountryModel_IndustryFraction
            WHERE BaseYear BETWEEN {first_year} AND {last_year}
        """
        cursor = self.db_connection.cursor()
        cursor.execute(delete_query)
        self.db_connection.commit()

        industry_fractions.to_sql('CountryModel_IndustryFraction', self.db_engine_economic_research, if_exists='append',
                                 index=False)


        # Country Model Version - GDP Remainders retrieving partition to be restored.
        # GDP remainders by region, for those regions where we don't have all the countries.
        sql_statement_GDP_rem = \
            f"SELECT [Year],[Region], [Country],[RemainderSize] FROM [dbo].[CMV_UEF_GDP_Remainder] " \
            f"WHERE [VersionKey] = '{versionKey}' " \
            f"ORDER BY [Region], [Country] "
        gdp_rem = pd.read_sql(sql_statement_GDP_rem, self.db_connection)

        delete_query = f"""
            DELETE FROM FinancialMetrics_GDP_Region_Remainder
            WHERE Year BETWEEN {first_year} AND {last_year}
        """
        cursor = self.db_connection.cursor()
        cursor.execute(delete_query)
        self.db_connection.commit()

        gdp_rem.to_sql('FinancialMetrics_GDP_Region_Remainder', self.db_engine_economic_research, if_exists='append',
                                  index=False)
        return "Restore Completed"

    # Restore -(TGF = Technology Group Factors) by Version Key
    def restore_version_techgroup_economic_research(self, versionKey:int, first_year:int, last_year:int ):

        # Country Model Versions storage table:  SQL statement to retrieve partition
        # Automation Degree by Country X Industry Archiving (TGF = Technology Group Factors)
        sql_statement_automation_degree = \
            f"SELECT [BaseYear],[Region], [Country],[Industry],[AutomationDegree], [TechnologyGroupID] FROM [dbo].[CMV_TGF_AutomationDegree] " \
            f"WHERE [VersionKey] = '{versionKey}' " \
            f"ORDER BY [Region], [Country] "

        automation_degree = pd.read_sql(sql_statement_automation_degree, self.db_connection)

        delete_query = f"""
            DELETE FROM CountryModel_AutomationDegree
            WHERE BaseYear BETWEEN {first_year} AND {last_year}
        """
        cursor = self.db_connection.cursor()
        cursor.execute(delete_query)
        self.db_connection.commit()

        automation_degree.to_sql('CountryModel_AutomationDegree', self.db_engine_economic_research, if_exists='append',
                                 index=False)

        # Country Model Versions storage table:  SQL statement to retrieve Industrial GDP partition
        # Industrial GDP Multiplier archiving (TGF = Technology Group Factors)
        sql_statement_industry_GDP_fraction = \
            f"SELECT [BaseYear],[Region], [Country],[IndustrialGDP_Fraction], [TechnologyGroupID] FROM [dbo].[CMV_TGF_IndustryGDP] " \
            f"WHERE [VersionKey] = '{versionKey}' " \
            f"ORDER BY [Region], [Country] "
        industry_gdp = pd.read_sql(sql_statement_industry_GDP_fraction, self.db_connection)

        delete_query = f"""
            DELETE FROM CountryModel_IndustryGDP
            WHERE BaseYear BETWEEN {first_year} AND {last_year}
        """
        cursor = self.db_connection.cursor()
        cursor.execute(delete_query)
        self.db_connection.commit()

        industry_gdp.to_sql('CountryModel_IndustryGDP', self.db_engine_economic_research, if_exists='append',
                                  index=False)

        return "Restore Completed"

    # Version Management Table for Technology Group Economic Factors
    def get_latest_version_techgroup_economic_research(self):
        sql_statement_cm_versions = \
            f"SELECT [Version],[Comment], [Modified],[VersionKey] FROM [dbo].[CMV_TGF_VersionManagement] " \
            f"ORDER BY [Version],[Modified] "
        version_table = pd.read_sql(sql_statement_cm_versions, self.db_connection)
        latest_version =  version_table['Version'].max() if not version_table.empty else None
        return latest_version

    # Version Management Table for Universal Economic Factors
    def get_latest_version_uef_research(self):
        sql_statement_cm_versions = \
            f"SELECT [Version],[Comment], [Modified],[VersionKey] FROM [dbo].[CMV_UEF_VersionManagement] " \
            f"ORDER BY [Version],[Modified] "
        version_table = pd.read_sql(sql_statement_cm_versions, self.db_connection)
        latest_version =  version_table['Version'].max() if not version_table.empty else None
        return latest_version

    def get_techgroup_version_table_economic_research(self):
        sql_statement_cm_versions = \
            f"SELECT [Version],[Comment],[YearFirst],[YearLast],[Modified],[VersionKey] FROM [dbo].[CMV_TGF_VersionManagement] " \
            f"ORDER BY [Version],[Modified] "
        version_table = pd.read_sql(sql_statement_cm_versions, self.db_connection)
        return version_table

    def get_uef_version_table_economic_research(self):
        sql_statement_cm_versions = \
            f"SELECT [Version],[YearFirst],[YearLast],[Comment], [Modified],[VersionKey] FROM [dbo].[CMV_UEF_VersionManagement] " \
            f"ORDER BY [Version],[Modified] "
        version_table = pd.read_sql(sql_statement_cm_versions, self.db_connection)
        return version_table

    def publish_industry_weights(self,industry_weights, year, region, country):
        sql_statement = """
              DELETE FROM [dbo].[CountryModel_IndustryFraction]
              WHERE [Region] = :region AND [BaseYear] = :year AND [Country] = :country
          """
        try:
            with self.db_engine_economic_research.begin() as connection:  # begin() ensures atomic transaction
                connection.execute(text(sql_statement), {"year": year, "region": region, "country": country})
                industry_factors = industry_weights.applymap(lambda x: x.strip() if isinstance(x, str) else x)
                industry_factors.to_sql('CountryModel_IndustryFraction', connection, if_exists='append', index=False)
        except Exception as e:
            print(f"Error executing SQL statement: {e}")
        return

    def publish_automation_degree(self,automation_degree, year, region, country):
        sql_statement = """
                DELETE FROM [dbo].[CountryModel_AutomationDegree]
                WHERE [Region] = :region AND [BaseYear] = :year AND [Country] = :country
            """
        try:
             with self.db_engine_economic_research.begin() as connection:
                connection.execute(text(sql_statement), {"year": year, "region": region, "country": country})
                automation_factors = automation_degree.applymap(lambda x: x.strip() if isinstance(x, str) else x)
                automation_factors.to_sql('CountryModel_AutomationDegree', connection, if_exists='append', index=False)
        except Exception as e:
            print(f"Error executing SQL statement: {e}")
        return

    def publish_region_gdp_fraction(self, gdp_fractions, year, region):
        sql_statement = """
                DELETE FROM [dbo].[CountryModel_IndustryGDP]
                WHERE [Region] = :region AND [BaseYear] = :year
            """
        try:
            with self.db_engine_economic_research.begin() as connection:
                connection.execute(text(sql_statement), {"year": year, "region": region})
                gdp_fractions = gdp_fractions.applymap(lambda x: x.strip() if isinstance(x, str) else x)
                gdp_fractions.to_sql('CountryModel_IndustryGDP', connection, if_exists='append', index=False)
        except Exception as e:
            print(f"Error executing SQL statement: {e}")
        return

    def publish_regional_gdp_remainders(self, gdp_remainders, year, region):
        sql_statement = """
                DELETE FROM [dbo].[FinancialMetrics_GDP_Region_Remainder]
                WHERE [Region] = :region AND [Year] = :year
            """
        try:
            with self.db_engine_economic_research.begin() as connection:
                connection.execute(text(sql_statement), {"year": year, "region": region})
                gdp_remainders = gdp_remainders.applymap(lambda x: x.strip() if isinstance(x, str) else x)
                gdp_remainders.to_sql('FinancialMetrics_GDP_Region_Remainder', connection, if_exists='append',
                                      index=False)
        except Exception as e:
            print(f"Error executing SQL statement: {e}")
        return

    def __init__(self):
        self.db_engine_economic_research = DatabaseConnections().get_MiraIndustry_engine()
        self.db_connection = DatabaseConnections().get_MiraIndustry_Connection()

# EconomicResearchFactors - Used to pull the tables for the Economic Research
#   There are 3 tables which are integral to the Country Model generation:
#       Automation_Factors
#       GDP_Fraction
#       Regional Remainders
#
# List of methods:
#   get_automation_degree
#   get_automation_degree_country
#   get_industry_gdp_fraction
#   get_industry_gdp_fraction_country
#   get_regional_remainder
#   get_regional_remainder_region
#
class Economic_Research_Factors:
    year = None
    country_model_industry_fraction = None
    country_model_industry_gdp_fraction = None
    country_model_industry_automation_degree = None
    regional_remainders = None
    economic_research_data = None
    gdp_x_country = None


    def get_automation_degree(self):
        return self.country_model_industry_automation_degree

    def get_industry_fractions_country(self,country):
        _industry_fractions = self.country_model_industry_fraction[(self.country_model_industry_fraction['Country'] == country)]
        return _industry_fractions

    def get_automation_degree_country(self,country):
        _automation_degree = self.country_model_industry_automation_degree[(self.country_model_industry_automation_degree['Country'] == country)]
        return _automation_degree

    def get_industry_gdp_fraction(self):
        return self.country_model_industry_gdp_fraction

    def get_industry_gdp_fraction_region(self,region):
        _automation_gdp = self.country_model_industry_gdp_fraction[(self.country_model_industry_gdp_fraction['Region'] == region)]
        return _automation_gdp

    def get_regional_remainder(self):
        return self.regional_remainders

    def get_regional_remainder_region(self,region):
        _region_remainders = self.regional_remainders[self.regional_remainders['Region']==region]
        return _region_remainders

    def __init__(self, year):
        self.connection = DatabaseConnections().get_MiraIndustry_Connection()
        self.year = year
        sql_statement_industry_fraction = \
            f"SELECT [BaseYear],[Region], [Country],[Industry],[IndustryFraction] FROM [dbo].[CountryModel_IndustryFraction] " \
            f"WHERE [BaseYear] = '{self.year}' " \
            f"ORDER BY [Region], [Country] "
        self.country_model_industry_fraction = pd.read_sql(sql_statement_industry_fraction, self.connection)
        sql_statement_automation_degree = \
            f"SELECT [BaseYear],[Region], [Country],[Industry],[AutomationDegree] FROM [dbo].[CountryModel_AutomationDegree] " \
            f"WHERE [BaseYear] = '{self.year}' " \
            f"ORDER BY [Region], [Country] "
        self.country_model_industry_automation_degree = pd.read_sql(sql_statement_automation_degree, self.connection)
        sql_statement_industryGDP = \
            f"SELECT [BaseYear],[Region],[Country],[IndustrialGDP_Fraction] FROM [dbo].[CountryModel_IndustryGDP] " \
            f"WHERE [BaseYear] = '{self.year}' " \
            f"ORDER BY [Country] "
        self.country_model_industry_gdp_fraction = pd.read_sql(sql_statement_industryGDP, self.connection)
        sql_statement_regional_rem = \
            f"SELECT [Year],[Region], [Country],[RemainderSize] FROM [dbo].[FinancialMetrics_GDP_Region_Remainder] " \
            f"WHERE [Year] = '{self.year}' " \
            f"ORDER BY [Region], [Country] "
        self.regional_remainders = pd.read_sql(sql_statement_regional_rem, self.connection)