import sys
import warnings
import pyodbc
import pandas as pd
import numpy as np
from openpyxl.descriptors import Integer
from pyodbc import STRING
import math

from myCountryModelPackages.MarketReportRetrieval import *
from myCountryModelPackages.Economic_Research import *

warnings.filterwarnings('ignore')


# This Class balances the original Country Model allocation of Region and Industry and
# aligns it with the Worldwide market report
class Country_Model_Forecast_RAS_Balancing:

    def __init__(self, cxcn, market_report:str, base_year:int, iterations:int):
        self.ABS_TOL = 0.01
        if iterations == None:
            self.max_iterations = 25
        else:
            self.max_iterations = iterations
        self.tolerance = 1e-7
        self.cxcn = cxcn
        self.base_year = base_year
        self.market_report = market_report
        self.market_report_data = MarketReportData(cxcn, market_report, base_year)
        self.worldwide_forecast = self.market_report_data.get_worldwide_forecast()
        self.worldwide_size = self.market_report_data.get_worldwide_size()
        self.country_model_size_original = self.market_report_data.get_country_model_size()
        self.country_model_forecast_original = self.market_report_data.get_country_model_forecast()
        self.country_model_size_aligned = []
        self.country_model_forecast_aligned = []
        return

    # Using the Country Model Market Size table after alignment, an Industry by Region table is built
    # for the base year of the report
    def __industry_by_region_base_year_alignment(self):
        industry_by_region_base_year = (
            self.country_model_size_aligned[self.country_model_size_aligned['BaseYear'] == self.base_year]
            .groupby(['BaseYear', 'Region', 'Industry'])['Size']
            .sum()
            .to_frame('Target_Value')
            .reset_index()
            .rename(columns={'BaseYear': 'Year'})
        )

        return industry_by_region_base_year

    def __industry_targets_forecast(self):
        target_reg = (
            self.worldwide_forecast.loc[
                (self.worldwide_forecast["Segment"] == "World Region") &
                (self.worldwide_forecast["Category"].isin(["Asia", "EMEA", "Latin America", "North America"]))
                ]
            .groupby(["Year", "Category"], as_index=False)["Forecast"]
            .sum()
            .rename(columns={"Category": "Region", "Forecast": "RegionTotal"})
        )
        industry_totals_by_year = (
            self.worldwide_forecast.loc[
                (self.worldwide_forecast["Segment"] == "Industry")
            ]
            .groupby(["Year", "Category"], as_index=False)["Forecast"]
            .sum()
            .rename(columns={"Category": "Industry", "Forecast": "IndustryTotal"})
        )
        _target_ind = industry_totals_by_year
        # In some cases the Worldwide market report totals for Industry and Region aren't perfectly aligned
        # This reconciliation ensures that the Industry totals align with the worldwide Regions.
        target_ind = self.__reconcile_totals_per_year(target_reg, _target_ind)
        return target_ind

    # Verification of Aligned Country Model Market Size with the Worldwide Market Size Report
    #  - Company Worldwide totals
    #  - Company by Region
    #  - Company by Worldwide Industry
    def verification_market_size_by_company_region_and_industry_with_worldwide(self):
        self.ABS_TOL = 0.01  # example: 1.0 units

        # Verify: Worldwide by Company Size
        company_totals_aligned = (
            self.country_model_size_aligned[self.country_model_size_aligned['Segment'] == 'World Region'].groupby(['Company','Segment'])['Size'].sum().reset_index())
        company_totals_target = (
            self.worldwide_size[self.worldwide_size['Segment'] == 'World Region'].groupby(['Company','Segment'])['Size'].sum().reset_index())
        comparison_company_totals = pd.merge(company_totals_aligned, company_totals_target, on='Company', suffixes=('_IPF', '_Target'))
        comparison_company_totals['Difference'] = (comparison_company_totals['Size_IPF'] - comparison_company_totals['Size_Target']).abs()
        comparison_company_totals['Exceeds_Tolerance'] = comparison_company_totals ['Difference'] > self.ABS_TOL
        over_tolerance_company_totals = comparison_company_totals.loc[comparison_company_totals['Exceeds_Tolerance']]

        # Verify: Company Region-Aligned vs Worldwide Report Company Region
        company_region_aligned = (
            self.country_model_size_aligned
            .loc[self.country_model_size_aligned['Segment'] == 'World Region']
            .groupby(['Company', 'Region'], as_index=False)['Size']
            .sum()
            .rename(columns={'Size': 'Size_IPF'})
        )

        # Build target totals from Worldwide report by Company & Region (Category -> Region)
        company_region_target = (
            self.worldwide_size
            .loc[self.worldwide_size['Segment'] == 'World Region']
            .groupby(['Company', 'Category'], as_index=False)['Size']
            .sum()
            .rename(columns={'Category': 'Region', 'Size': 'Size_Target'})
        )

        # Merge on Company AND Region
        comparison_company_region = pd.merge(
            company_region_aligned, company_region_target,
            on=['Company', 'Region'], how='outer'
        )

        comparison_company_region[['Size_Aligned', 'Size_Target']] = (
            comparison_company_region[['Size_IPF', 'Size_Target']].fillna(0.0)
        )
        comparison_company_region['Difference'] = (comparison_company_region['Size_Aligned'] - comparison_company_region['Size_Target']).abs()
        comparison_company_region['Exceeds_Tolerance'] = comparison_company_region ['Difference'] > self.ABS_TOL
        over_tolerance_company_region = comparison_company_region.loc[comparison_company_region['Exceeds_Tolerance']]

        # Verify: Company Industry-Aligned vs Worldwide Report Company Industry
        company_industry_aligned = self.country_model_size_aligned[self.country_model_size_aligned['Segment'] == 'World Region'].groupby(['Company','Industry'])['Size'].sum().reset_index()
        company_industry_target = self.worldwide_size[self.worldwide_size['Segment'] == 'Industry'].groupby(['Company','Category'])['Size'].sum().reset_index()
        company_industry_target = company_industry_target.rename(columns={'Category': 'Industry'})
        comparison_company_industry = pd.merge(company_industry_aligned, company_industry_target, on=['Company','Industry'], suffixes=('_IPF', '_Target'))
        comparison_company_industry['Difference'] = (comparison_company_industry['Size_IPF'] - comparison_company_industry['Size_Target']).abs()
        comparison_company_industry['Exceeds_Tolerance'] = comparison_company_industry ['Difference'] > self.ABS_TOL
        over_tolerance_company_industry = comparison_company_industry.loc[comparison_company_industry['Exceeds_Tolerance']]
        # Verify: Regional Totals
        #  - don't think this is necessary
        # Verify: Industry Totals Worldwide
        #  - don't think this is necessary
        return over_tolerance_company_totals, over_tolerance_company_region, over_tolerance_company_industry

    # Verification of Aligned Country Model Market Forecast with the Worldwide Market Forecast Report
    #  - 1st year alignment with Country Model Size that was aligned
    #  - Industry totals for all forecast years
    #  - Region totals for all forecast yearsCompany by Worldwide Industry
    def verification_market_forecast_by_region_and_industry_with_worldwide(self):
        #1 Verify: 1st Year Alignment of Industry by Region (market size & market forecast)
        #       After the Country Model Market Size is aligned, we will use this to determine the Regional Industries
        #       It is only in the Base Year case that this is necessary.
        industry_by_region_cm_size_target = self.__industry_by_region_base_year_alignment()
        industry_by_region_cm_forecast_base_year_aligned = (
            self.country_model_forecast_aligned[self.country_model_forecast_aligned['Year'] == self.base_year].groupby(['Region','Industry'])['Forecast'].sum().reset_index())
        comparison_industry_region_base_year_totals = pd.merge(industry_by_region_cm_forecast_base_year_aligned, industry_by_region_cm_size_target, on=['Region','Industry'])
        comparison_industry_region_base_year_totals['Difference'] = (comparison_industry_region_base_year_totals['Forecast'] - comparison_industry_region_base_year_totals['Target_Value']).abs()
        comparison_industry_region_base_year_totals['Exceeds_Tolerance'] = comparison_industry_region_base_year_totals ['Difference'] > self.ABS_TOL
        over_tolerance_industry_x_region_base_year = comparison_industry_region_base_year_totals.loc[comparison_industry_region_base_year_totals['Exceeds_Tolerance']]

        #2 Verify: Industry Totals - Country Model Forecast for Each Forecast Year vs  Worldwide Industry Totals
        industry_total_cm_forecast_aligned = (
            self.country_model_forecast_aligned[self.country_model_forecast_aligned['Year'] >= self.base_year].groupby(['Year','Industry'])['Forecast'].sum().reset_index())
#        industry_total_target = (
#            self.worldwide_forecast[self.worldwide_forecast[['Segment'] == 'Industry'],['Year' > self.base_year]].groupby(['Year','Category'])['Forecast'].sum().reset_index().rename(columns={'Category': 'Industry'}))
#        industry_total_target = (
#            self.worldwide_forecast
#            .query("Segment == 'Industry' and Year > @self.base_year")
#            .groupby(['Year', 'Category'], as_index=False)['Forecast']
#            .sum()
#            .rename(columns={'Category': 'Industry'})
#        )
        industry_total_target = (
            self.worldwide_forecast
            .loc[
                (self.worldwide_forecast['Segment'] == 'Industry') &
                (self.worldwide_forecast['Year'] > self.base_year)
                ]
            .groupby(['Year', 'Category'], as_index=False)['Forecast']
            .sum()
            .rename(columns={'Category': 'Industry'})
        )
        _reconciled_industry_targets = self.__industry_targets_forecast()
        comparison_industry_totals = pd.merge(industry_total_cm_forecast_aligned, _reconciled_industry_targets,on=['Year', 'Industry'], suffixes=('_IPF', '_Target'))
        comparison_industry_totals['Difference'] = (comparison_industry_totals['Forecast'] - comparison_industry_totals['IndustryTotal']).abs()
        comparison_industry_totals['Exceeds_Tolerance'] = comparison_industry_totals ['Difference'] > self.ABS_TOL
        over_tolerance_industry_totals  = comparison_industry_totals.loc[comparison_industry_totals['Exceeds_Tolerance']]

        #3 Verify: Region Totals - Country Model Forecast for each Forecast Year vs Worldwide Region Totals
        region_total_cm_forecast_aligned = (
            self.country_model_forecast_aligned[self.country_model_forecast_aligned['Year'] > self.base_year].groupby(
                ['Year', 'Region'])['Forecast'].sum().reset_index())

        region_total_target = (
            self.worldwide_forecast
            .loc[
                (self.worldwide_forecast['Segment'] == 'World Region') &
                (self.worldwide_forecast['Year'] > self.base_year)
                ]
            .groupby(['Year', 'Category'], as_index=False)['Forecast']
            .sum()
            .rename(columns={'Category': 'Region'})
        )

        comparison_region_totals = pd.merge(region_total_cm_forecast_aligned, region_total_target,
                                              on=['Year', 'Region'], suffixes=('_IPF', '_Target'))
        comparison_region_totals['Difference'] = (
                    comparison_region_totals['Forecast_IPF'] - comparison_region_totals['Forecast_Target']).abs()
        comparison_region_totals['Exceeds_Tolerance'] = comparison_region_totals['Difference'] > self.ABS_TOL
        over_tolerance_region_totals = comparison_region_totals.loc[comparison_region_totals['Exceeds_Tolerance']]
        return over_tolerance_industry_x_region_base_year, over_tolerance_industry_totals, over_tolerance_region_totals


    def ipf_align_country_model(self):
        model_aligned_share, iter_share = self.align_market_size_by_company_region_and_industry_with_worldwide()

        industry_by_region_base_year = (
            model_aligned_share[model_aligned_share['BaseYear'] == self.base_year]
            .groupby(['BaseYear', 'Region', 'Industry'])['Size']
            .sum()
            .to_frame('Target_Value')
            .reset_index()
            .rename(columns={'BaseYear': 'Year'})
        )
        model_aligned_forecast, iter_x_year = self.align_forecast_by_region_and_industry(industry_by_region_base_year)

        return model_aligned_share, model_aligned_forecast, iter_share, iter_x_year

    def align_market_size_by_company_region_and_industry_with_worldwide(self):
        """
        df_detail columns: Year, Region, Country, Industry, Forecast
        region_totals_by_year: Year, Region, RegionTotal
        industry_totals_by_year: Year, Industry, IndustryTotal
        """
       # market_report_data = MarketReportData(cxcn, market_report, base_year)

        # retrieve the Generated Country Model by Base Year
        # chane the names of the columns for sanity (Region, Industry, Year, Value)
     #   country_model_size_original = self.market_report_data.get_country_model_size()
        country_model_size = self.country_model_size_original
        country_model_size['Size'] = country_model_size['Size'].replace(0.0, 1e-9)
       # retrieve the Worldwide report to create a two tables
        # Region and Industry totals by calendar year
    #    worldwide_size = self.market_report_data.get_worldwide_size()

        df_regions = self.worldwide_size[self.worldwide_size['Segment'] == 'World Region'].copy()
        df_industries = self.worldwide_size[self.worldwide_size['Segment'] == 'Industry'].copy()
        targets = {
            ('Company',): self.worldwide_size[self.worldwide_size['Segment'] == 'World Region'].groupby('Company')[
                'Size'].sum().to_frame('Target_Value'),
            ('Company', 'Region'): df_regions.groupby(['Company', 'Category'])['Size'].sum().to_frame(
                'Target_Value').rename_axis(['Company', 'Region']),
            ('Region',): df_regions.groupby('Category')['Size'].sum().to_frame('Target_Value').rename_axis(['Region']),
            ('Industry',): df_industries.groupby('Category')['Size'].sum().to_frame('Target_Value').rename_axis(
                ['Industry']),
            # New Constraint added directly
            ('Company', 'Industry'): df_industries.groupby(['Company', 'Category'])['Size'].sum().to_frame(
                'Target_Value').rename_axis(['Company', 'Industry'])
        }
        # The 'Total Global Size' must be the same in all three views
        sum_comp = targets[('Company',)]['Target_Value'].sum()
        sum_reg = targets[('Region',)]['Target_Value'].sum()
        sum_ind = targets[('Industry',)]['Target_Value'].sum()
        sum_comp_ind = targets[('Company', 'Industry')]['Target_Value'].sum()

        print(f"Check: {sum_comp:.2f} == {sum_reg:.2f} == {sum_ind:.2f}")
        if not (math.isclose(sum_comp, sum_ind, abs_tol=0.011) and
                math.isclose(sum_comp, sum_comp_ind, abs_tol=0.011)):
            print("CRITICAL: Global totals mismatch.")
        # 2. Check Detail Consistency (Company by Industry vs. Company Total)
        # Group the 2D target by Company and compare to the 1D Company target
        comp_from_ind = targets[('Company', 'Industry')].groupby('Company')['Target_Value'].sum()
        comp_direct = targets[('Company',)]['Target_Value']

        # Find any companies where the industry sum doesn't match the company total
        mismatches = comp_from_ind[~np.isclose(comp_from_ind, comp_direct)]
        if not mismatches.empty:
            print(
                f"Warning: Industry totals for these companies don't match Company totals: {mismatches.index.tolist()}")

        for n_iter in range(self.max_iterations):
            # 0. Store current values for convergence check
            prev_sizes = country_model_size['Size'].copy()

            # 1. Apply each constraint in the targets dictionary
            for dims, target_df in targets.items():
                # Calculate current sum for this specific grouping (e.g., Company/Region)
                current_sum = country_model_size.groupby(list(dims))['Size'].transform('sum')

                # Calculate the adjustment factor (Target / Current)
                factors = country_model_size.merge(target_df, on=list(dims), how='left')['Target_Value'] / (
                            current_sum + 1e-12)

                # Multiply the cell values by the factor
                country_model_size['Size'] *= factors.fillna(1.0)

            # 2. Check for convergence
            max_diff = np.abs(country_model_size['Size'] - prev_sizes).max()

           # if n_iter % 10 == 0:
           #     st.write(f"Iteration {n_iter}: Max change = {max_diff:.8f}")

            if max_diff < self.tolerance:
                # st.write(f"--- SUCCESS: Matrix balanced at iteration {n_iter} ---")
                break
        self.country_model_size_aligned = country_model_size
        return country_model_size, n_iter

    def __reconcile_totals_per_year(self, region_targets, industry_targets):
        """
        Each input is a DataFrame with columns ['Year', key, 'Target_Value']
        (key = 'Region' for region_targets, 'Industry' for industry_targets).
        Returns a copy of industry_targets scaled so that, year-by-year,
        sum(Industry) == sum(Region). If Region is the one you trust more,
        scale Industry; swap roles if needed.
        """
        ind = industry_targets.copy()
        # Compute grand totals per year
        r_sum = region_targets.groupby('Year', as_index=True)['RegionTotal'].sum().rename('Rsum')
        i_sum = ind.groupby('Year', as_index=True)['IndustryTotal'].sum().rename('Isum')
        factor = (r_sum / i_sum).replace([np.inf, -np.inf], np.nan).fillna(1.0)

        # Map year-wise factor to rows
        ind['Scale'] = ind['Year'].map(factor)
        ind['IndustryTotal'] = ind['IndustryTotal'] * ind['Scale']
        ind = ind.drop(columns=['Scale'])
        return ind

    def __forcast_convergence_check(self, ipf_update, target):
        # Check for convergence
        _compare_with_target = pd.merge(ipf_update, target, on=['Segment'],
                                                   suffixes=('_IPF', '_Target'))
        _compare_with_target['Difference'] = (
                _compare_with_target['Forecast'] - _compare_with_target['TargetTotal']).abs()
        _compare_with_target['Exceeds_Tolerance'] = _compare_with_target[
                                                                   'Difference'] > self.tolerance
        over_tolerance_industry_totals = _compare_with_target.loc[
            _compare_with_target['Exceeds_Tolerance']]
        return over_tolerance_industry_totals

    def align_forecast_by_region_and_industry(self, industry_by_region_base_year):
        """
        df_detail columns: Year, Region, Country, Industry, Forecast
        region_totals_by_year: Year, Region, RegionTotal
        industry_totals_by_year: Year, Industry, IndustryTotal
        """
       # market_report_data = MarketReportData(cxcn, market_report, base_year)

        # retrieve the Generated Country Model by Base Year
        # chane the names of the columns for sanity (Region, Industry, Year, Value)
       # self.country_model_forecast = self.market_report_data.get_country_model_forecast()

        # retrieve the Worldwide report to create a two tables
        # Region and Industry totals by calendar year
      #  worldwide_forecast = self.market_report_data.get_worldwide_forecast()

        region_totals_by_year = (
            self.worldwide_forecast.loc[
                (self.worldwide_forecast["Segment"] == "World Region") &
                (self.worldwide_forecast["Category"].isin(["Asia", "EMEA", "Latin America", "North America"]))
                ]
            .groupby(["Year", "Category"], as_index=False)["Forecast"]
            .sum()
            .rename(columns={"Category": "Region", "Forecast": "RegionTotal"})
        )

        industry_totals_by_year = (
            self.worldwide_forecast.loc[
                (self.worldwide_forecast["Segment"] == "Industry")
            ]
            .groupby(["Year", "Category"], as_index=False)["Forecast"]
            .sum()
            .rename(columns={"Category": "Industry", "Forecast": "IndustryTotal"})
        )

        all_years = sorted(self.country_model_forecast_original['Year'].unique())
        iterations_x_year = pd.DataFrame({'Year': all_years})
        iterations_x_year['Iterations'] = 0

        first_year = all_years[0]

        # List to store each aligned year
        aligned_years_list = []

        for yr in all_years:
            print(f"\n--- Processing Year: {yr} ---")

            # 1. Slice the main dataframe for the current year
            # We work on a copy to avoid SettingWithCopyWarnings
            country_model_forecast_yr = self.country_model_forecast_original[self.country_model_forecast_original['Year'] == yr].copy()

            # 2. Prepare Targets for this specific year
            target_reg = region_totals_by_year[region_totals_by_year['Year'] == yr].set_index('Region')
            _target_ind = industry_totals_by_year[industry_totals_by_year['Year'] == yr].set_index('Industry')
            # In some cases the Worldwide market report totals for Industry and Region aren't perfectly aligned
            # This reconciliation ensures that the Industry totals align with the worldwide Regions.
            target_ind = self.__reconcile_totals_per_year(target_reg,  _target_ind )

            year_targets = {
                ('Region',): target_reg['RegionTotal'].to_frame('Target_Value'),
                ('Industry',): target_ind['IndustryTotal'].to_frame('Target_Value')
            }

            if yr == first_year:
                target_ind_reg = industry_by_region_base_year[industry_by_region_base_year['Year'] == yr]
                year_targets[('Region', 'Industry')] = target_ind_reg.set_index(['Region', 'Industry'])[
                    ['Target_Value']]
                #st.write(f"Forecasting (base year) - Applying 3 constraints: Region, Industry, and Industry x Region")
           # else:
                #st.write(f"Forecasting (forecast years) - Applying 2 constraints: Region and Industry")

            # 3. INTERNAL ALIGNMENT LOOP (IPF) for the current year
            for iteration in range(self.max_iterations):
                prev_forecast = country_model_forecast_yr['Forecast'].copy()  # Use 'Forecast' or your actual value column

                for dims, target_df in year_targets.items():
                    # Calculate current sums for the subset
                    current_sum = country_model_forecast_yr.groupby(list(dims))['Forecast'].transform('sum')

                    # Calculate factors - ensuring we join on the specific dimensions
                    # Using 'left' join to keep df_yr structure
                    merged = country_model_forecast_yr.merge(target_df, left_on=list(dims), right_index=True, how='left')
                    factors = merged['Target_Value'] / (current_sum + 1e-12)

                    # Apply adjustment
                    country_model_forecast_yr['Forecast'] *= factors.fillna(1.0)

                # Check for convergence by Industry and Region alignments
                _industry_totals_cm_forecast_aligned = (
                    country_model_forecast_yr.groupby(
                        ['Industry'])['Forecast'].sum().reset_index())
                _industry_totals_cm_forecast_aligned.index.name = "Industry"

                over_tolerance_industry_totals = \
                    self.__forcast_convergence_check(
                        _industry_totals_cm_forecast_aligned.rename(columns={'Industry': 'Segment'}),
                        target_ind.rename(columns={'IndustryTotal': 'TargetTotal'})
                        .rename_axis(index='Segment'))


                _region_totals_cm_forecast_aligned = (
                    country_model_forecast_yr.groupby(
                        ['Region'])['Forecast'].sum().reset_index())
                over_tolerance_region_totals = \
                    self.__forcast_convergence_check(
                        _region_totals_cm_forecast_aligned.rename(columns={'Region': 'Segment'}),
                        target_reg.rename(columns={'RegionTotal': 'TargetTotal'})
                        .rename_axis(index='Segment'))

                if (over_tolerance_region_totals is not None and over_tolerance_region_totals.empty) and \
                        (over_tolerance_industry_totals is not None and over_tolerance_industry_totals.empty):
                    break

            # 4. Store the result
            aligned_years_list.append(country_model_forecast_yr)
            iterations_x_year.loc[iterations_x_year['Year'] == yr, 'Iterations'] = iteration
            iterations_x_year['Iterations'] = iterations_x_year['Iterations'].astype('Int64')

        # 5. Recombine all years into the final dataframe
        final_aligned_forecast = pd.concat(aligned_years_list)
        self.country_model_forecast_aligned = final_aligned_forecast
        return final_aligned_forecast, iterations_x_year