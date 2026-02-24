import sys
import warnings
import pyodbc
import pandas as pd
import numpy as np
import time
import sqlalchemy
import streamlit as st
from pandas.testing import assert_frame_equal
from myCountryModelPackages.Economic_Research import *

warnings.filterwarnings('ignore')

# Merge Invalid Industries in the Worldwide forecast or size data table into Other Industries
#   This function will take a dataframe for either the Forecast or Size sheet of a worldwide market report
#   and merge all the Industries that are not included in the Economic modeling and merge them into the Other Industries
def _merge_invalid_industries_with_other_in_worldwide_dataset(base_year, model_data):
    OTHER_INDUSTRY = "Other Industries"
    # retrieve a list of Industries that are supported in the Economic Model
    economic_research = CountryEconomicResearch(base_year, 0)
    valid_industry_list = economic_research.get_IndustryList()
    # --- 2) Pick your measure column ---
    value_col = 'Size' if 'Size' in model_data.columns else 'Forecast'

    # --- 3) Only reclassify inside Segment == "Industry" ---
    m_seg = model_data['Segment'] == 'Industry'
    m_cat_valid = model_data['Category'].isin(valid_industry_list)

    valid_industry = model_data[m_seg & m_cat_valid].copy()
    invalid_industry = model_data[m_seg & ~m_cat_valid].copy()
    other_segments = model_data[~m_seg].copy()  # leave non-Industry segments unchanged

    # --- 4) If no invalids under Segment=Industry, just return the original dataframe ---
    if invalid_industry.empty:
        _other_industries_merged = model_data #pd.concat([valid_industry, other_segments], ignore_index=True)

    else:
        # 5) Grouping:
        #       - Market Size - Company/Segment/
        #       - Market Forecast - Year/Segment;
        #       Exclude the columns we replace (Category/Parent/GrandParent), and the measure column
        drop_cols = {'Category', 'ParentCategory', 'GrandParentCategory', value_col, 'Units'}
        group_keys = [c for c in model_data.columns if c not in drop_cols]

        # (Optional) If you want to be explicit and safe, you can construct it like:
        # group_keys = ['Study','BaseYear','Year','Region','Country','Company','Segment','StudyId', ...]
        # ...keeping every dimension you need, especially 'Company'.

        # --- 6) Aggregate invalid industries per Company (and per the rest of the grain) ---
        other_bucket = (
            invalid_industry
            .groupby(group_keys, dropna=False, as_index=False)[value_col]
            .sum()
        )

        # --- 7) Set the reclassified labels ---
        other_bucket['Category'] = OTHER_INDUSTRY

        # Ensure Units (or any required metadata) are filled
        if 'Units' in model_data.columns:
            units_fill = model_data['Units'].mode().iat[0] if not model_data['Units'].empty else None
            other_bucket['Units'] = units_fill

        # 1) Split valid_industry into "other" and "not other"
        valid_other = valid_industry.loc[valid_industry['Category'] == OTHER_INDUSTRY].copy()
        valid_no_other = valid_industry.loc[valid_industry['Category'] != OTHER_INDUSTRY].copy()

        # 2) Ensure other_bucket has Category properly set (and any parents if you use them)
        #other_bucket = other_bucket.copy()
        #other_bucket['Category'] = OTHER_INDUSTRY
        # other_bucket['ParentCategory'] = ''
        # other_bucket['GrandParentCategory'] = ''

        # 3) Align both "Other_industries" sets to the same columns (keys + measure + required metadata)
        #    We’ll keep only the grouping keys + measure here, then re-attach the constant labels.
        keep_cols_for_sum = [*group_keys, value_col]
        v_other_sum = (
            valid_other[keep_cols_for_sum]
            .groupby(group_keys, dropna=False, as_index=False)[value_col].sum()
        )
        o_bucket_sum = (
            other_bucket[keep_cols_for_sum]
            .groupby(group_keys, dropna=False, as_index=False)[value_col].sum()
        )

        # 4) Combine (sum) the two "Other" sources
        #    Use an outer merge on the keys, fill NaNs with 0, add measures, and keep one measure column.
        merged_other = (
            v_other_sum
            .merge(o_bucket_sum, on=group_keys, how='outer', suffixes=('_valid', '_agg'))
            .fillna({f'{value_col}_valid': 0, f'{value_col}_agg': 0})
        )

        merged_other[value_col] = merged_other[f'{value_col}_valid'] + merged_other[f'{value_col}_agg']
        merged_other = merged_other.drop(columns=[f'{value_col}_valid', f'{value_col}_agg'])

        # 5) Re-attach the "Other_industries" labels and any constant metadata (e.g., Units)
        merged_other['Category'] = OTHER_INDUSTRY
        merged_other['ParentCategory'] = ''

        if 'Units' in valid_industry.columns:
            # choose a sensible value; mode() is a good default if Units is consistent
            units_value = valid_industry['Units'].mode().iat[0] if not valid_industry['Units'].empty else None
            merged_other['Units'] = units_value

        # 6) Compose the final table:
        #    - valid non-Other rows
        #    - combined Other rows
        #    - all other segments (non-Industry) unchanged
        _other_industries_merged = pd.concat(
            [valid_no_other, merged_other, other_segments],
            ignore_index=True
        )
    return _other_industries_merged

def _merge_world_market_forecast_invalid_industries_with_other(base_year, model_data):
    OTHER_INDUSTRY = "Other Industries"

    economic_research = CountryEconomicResearch(base_year, 0)
    valid_industry_list = economic_research.get_IndustryList()
    # --- 2) Pick your measure column ---
    value_col = 'Forecast' if 'Forecast' in model_data.columns else 'Value'

    # --- 3) Only reclassify inside Segment == "Industry" ---
    m_seg = model_data['Segment'] == 'Industry'
    m_cat_valid = model_data['Category'].isin(valid_industry_list)

    valid_industry = model_data[m_seg & m_cat_valid].copy()
    invalid_industry = model_data[m_seg & ~m_cat_valid].copy()
    other_segments = model_data[~m_seg].copy()  # leave non-Industry segments unchanged

    # --- 4) If no invalids under Segment=Industry, you’re done ---
    if invalid_industry.empty:
        _other_industries_merged = pd.concat([valid_industry, other_segments], ignore_index=True)

    else:
        # --- 5) Grouping grain: include Company (required), plus your ID fields
        #      Exclude the columns we replace (Category/Parent/GrandParent), and the measure column
        drop_cols = {'Category', 'ParentCategory', 'GrandParentCategory', value_col, 'Units'}
        group_keys = [c for c in model_data.columns if c not in drop_cols]

        # (Optional) If you want to be explicit and safe, you can construct it like:
        # group_keys = ['Study','BaseYear','Year','Region','Country','Company','Segment','StudyId', ...]
        # ...keeping every dimension you need, especially 'Company'.

        # --- 6) Aggregate invalid industries per Company (and per the rest of the grain) ---
        other_bucket = (
            invalid_industry
            .groupby(group_keys, dropna=False, as_index=False)[value_col]
            .sum()
        )

        # --- 7) Set the reclassified labels ---
        other_bucket['Category'] = 'Other_industries'

        # Ensure Units (or any required metadata) are filled
        if 'Units' in model_data.columns:
            units_fill = model_data['Units'].mode().iat[0] if not model_data['Units'].empty else None
            other_bucket['Units'] = units_fill

        # 1) Split valid_industry into "other" and "not other"
        valid_other = valid_industry.loc[valid_industry['Category'] == OTHER_INDUSTRY].copy()
        valid_no_other = valid_industry.loc[valid_industry['Category'] != OTHER_INDUSTRY].copy()

        # 2) Ensure other_bucket has Category properly set (and any parents if you use them)
        other_bucket = other_bucket.copy()
        other_bucket['Category'] = OTHER_INDUSTRY
        # other_bucket['ParentCategory'] = ''
        # other_bucket['GrandParentCategory'] = ''

        # 3) Align both "Other_industries" sets to the same columns (keys + measure + required metadata)
        #    We’ll keep only the grouping keys + measure here, then re-attach the constant labels.
        keep_cols_for_sum = [*group_keys, value_col]
        v_other_sum = (
            valid_other[keep_cols_for_sum]
            .groupby(group_keys, dropna=False, as_index=False)[value_col].sum()
        )
        o_bucket_sum = (
            other_bucket[keep_cols_for_sum]
            .groupby(group_keys, dropna=False, as_index=False)[value_col].sum()
        )

        # 4) Combine (sum) the two "Other" sources
        #    Use an outer merge on the keys, fill NaNs with 0, add measures, and keep one measure column.
        merged_other = (
            v_other_sum
            .merge(o_bucket_sum, on=group_keys, how='outer', suffixes=('_valid', '_agg'))
            .fillna({f'{value_col}_valid': 0, f'{value_col}_agg': 0})
        )

        merged_other[value_col] = merged_other[f'{value_col}_valid'] + merged_other[f'{value_col}_agg']
        merged_other = merged_other.drop(columns=[f'{value_col}_valid', f'{value_col}_agg'])

        # 5) Re-attach the "Other_industries" labels and any constant metadata (e.g., Units)
        merged_other['Category'] = OTHER_INDUSTRY
        merged_other['ParentCategory'] = ''
        merged_other['GrandParentCategory'] = ''

        if 'Units' in valid_industry.columns:
            # choose a sensible value; mode() is a good default if Units is consistent
            units_value = valid_industry['Units'].mode().iat[0] if not valid_industry['Units'].empty else None
            merged_other['Units'] = units_value

        # 6) Compose the final table:
        #    - valid non-Other rows
        #    - combined Other rows
        #    - all other segments (non-Industry) unchanged
        _other_industries_merged = pd.concat(
            [valid_no_other, merged_other, other_segments],
            ignore_index=True
        )
    return _other_industries_merged

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
        return self.base_year_list    #market_reports['BaseYear'].drop_duplicates()

    def get_country_model_report_list(self):
        return self.cm_market_reports

    def get_country_model_base_year_list(self):
        return self.cm_base_year_list

    def __init__(self,cxcn ):
        worldSegment = "World Region"
        industrySegment = "Industry"
        categoryName = "%"
        marketStudy = "%"
        year = "%"
        self.connection = cxcn
        self.sql_query_market_size = \
            f"SELECT [Study], [BaseYear] FROM [dbo].[StudyForecasts] " \
            f"WHERE   ([Study] LIKE '{marketStudy}') AND ([BaseYear] LIKE  '{year}') AND (([Segment] LIKE '{worldSegment}')  OR ([Segment] LIKE '{industrySegment}'))" \
            f"AND ([Category] LIKE '{categoryName}')" \
            f"ORDER BY [BaseYear]"

        self.dbo = "StudyForecastsCountryModel"
        self.categoryName = "North America"
        self.sql_query_country_models = \
            f"SELECT [Study], [BaseYear] FROM [dbo].[{self.dbo}] " \
            f"WHERE   ([Study] LIKE '{marketStudy}') AND ([BaseYear] LIKE  '{year}') AND ([BaseYear] LIKE  [Year]) " \
            f"AND (([Segment] LIKE '{worldSegment}')) " \
            f"AND ([Category] LIKE '{self.categoryName}') " \
            f"ORDER BY [BaseYear]"

        try:
            start_time = time.time()
            self.market_reports = pd.read_sql(self.sql_query_market_size, self.connection)
            elapsed_time = time.time() - start_time
            self.cm_market_reports = pd.read_sql(self.sql_query_country_models, self.connection)
            status = "success"
        except sqlalchemy.exc.OperationalError as e:
            st.write("Operational error:", e)
            status = "timeout or busy"
        except sqlalchemy.exc.ProgrammingError as e:
            st.write("Programming error:", e)
            status = "syntax or logic error"
        except Exception as e:
            st.write("Unexpected error:", e)
            status = "unknown error"

        print("Query status:", status)

        self.base_year_list =  \
            sorted(
            self.market_reports['BaseYear'].drop_duplicates().tolist(),
            reverse=True
            )
        self.cm_base_year_list =  \
            sorted(
            self.cm_market_reports['BaseYear'].drop_duplicates().tolist(),
            reverse=True
        )

        self.base_year_list = self.market_reports['BaseYear'].drop_duplicates().tolist()
        self.market_reports = self.market_reports[['BaseYear', 'Study']].drop_duplicates()

        self.cm_base_year_list = self.cm_market_reports['BaseYear'].drop_duplicates().tolist()
        self.cm_market_reports = self.cm_market_reports[['BaseYear', 'Study']].drop_duplicates()
# MarketReportData class is used to pull market report data from either the worldwide or country model tables in the SQL database.
# Currently, it only allows for one market report retrieval at a time.  Although this could easily be extended.
class MarketReportData:
    market_report = None
    base_year= None

    def get_worldwide_size(self):
        OTHER_INDUSTRY = "Other Industries"
        sql_query_market_size = \
            f"SELECT [Study], [BaseYear],[Company],[Segment],[Category],[ParentCategory],[Size] FROM [dbo].[StudySizes] " \
            f"WHERE   ([Study] = '{self.market_report}') AND ([BaseYear] = '{self.base_year}') " \
            f"AND ([Units] = 'Revenues') AND (([Segment] = 'Industry') OR ([Segment] = 'World Region')) " \
            f"ORDER BY  [Company]"
        model_data = pd.read_sql(sql_query_market_size, self.connection)

        market_data_other_industries_merged = _merge_invalid_industries_with_other_in_worldwide_dataset (self.base_year, model_data)

        return market_data_other_industries_merged

    def get_worldwide_forecast(self):
        sql_query_market_forecast = \
            f"SELECT [Study], [BaseYear],[Year],[Segment],[Category],[ParentCategory],[GrandParentCategory],[Forecast] FROM [dbo].[StudyForecasts] " \
            f"WHERE   ([Study] = '{self.market_report}') AND ([BaseYear] = '{self.base_year}') AND ([Units] = 'Revenues')" \
            f"AND (([Segment] = 'Industry') OR ([Segment] = 'World Region')) " \
            f"ORDER BY  [Segment], [Category], [Year]"
        model_data = pd.read_sql(sql_query_market_forecast, self.connection)
        market_data_other_industries_merged = _merge_invalid_industries_with_other_in_worldwide_dataset(self.base_year, model_data)
 #       market_data_other_industries_merged_2 = _merge__invalid_industries_with_other_in_worldwide_dataset(self.base_year,model_data)
 #       comparison_df =assert_frame_equal(market_data_other_industries_merged, market_data_other_industries_merged_2, check_dtype=False)  # set options you need

        return market_data_other_industries_merged


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

    def get_country_known_sizes_list(self):
        queryCountryKnown = f"SELECT [Study], [BaseYear],[Country],[Industry],[Company], [Size] FROM [dbo].[CountrySizes]" \
                            f" WHERE   (([Study] = '{self.market_report}') AND ([BaseYear] =  {self.base_year}))" \
                            f" ORDER BY [Study], [BaseYear], [Company], [Country], [Industry]"
        country_known_size = pd.read_sql(queryCountryKnown, self.connection)
        #if country_known_size.empty:
        country_known_summary = country_known_size.groupby('Country', as_index=False)['Size'].sum()
        country_known_summary['Country'] = country_known_summary['Country'].str.strip()

        return country_known_summary

    def __init__(self,cxcn, market_report, base_year ):
        self.connection = cxcn
        self.market_report = market_report
        self.base_year = base_year
