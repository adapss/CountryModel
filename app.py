import os
import time
import pandas as pd
import streamlit as st
import pyodbc

from myCountryModelPackages.CountryModel_Generation import *
from myCountryModelPackages.MarketReportRetrieval import MarketReports
from myCountryModelPackages.sqlTableRetrieve import *
from myCountryModelPackages.MarketReportRetrieval import *
from myCountryModelPackages.CountryModelEvaluationTools import *

if __name__ == '__main__':
    st.title("Country Model Generator")
    st.write("This application is designed to generated a Country Model from an previously published World Wide market Report.")
    db_cxcn = sqlConnection().get_connection()
    market_reports = MarketReports(db_cxcn)
    report_list = market_reports.get_report_list()
    report_list = report_list.sort_values(by='Study', ascending=True)
    base_year_list = market_reports.get_base_year_list().sort_values(ascending=False)

    # Display the Report List
    st.write('Market Report Selection:')
    selected_base_year = st.selectbox(f'Select Base Year', base_year_list)
    filtered_df = report_list[report_list['BaseYear'] == selected_base_year]
    # selected_report = st.multiselect('Select report:', filtered_df['Study'])
    selected_report = st.selectbox('Select report:', filtered_df['Study'])
    # Add buttons for user actions
    col1, col2 = st.columns(2)
    with col1:
        st.caption("Press the button to generated Country Model tables")
    col3, col4, col5, col6,col7,col8 = st.columns(6)
    with col1:
        if st.button('Create Country Model'):
           with st.spinner("Loading data..."):
                country_model = Country_Model_Generation( db_cxcn, selected_report, selected_base_year)
                share_model = country_model.generate_market_shares()
                forecast_model = country_model.generate_forecast()
                sql_market_data = MarketReportData(db_cxcn, selected_report, selected_base_year)
                sql_country_model_size = sql_market_data.get_country_model_size()
                sql_country_model_forecast = sql_market_data.get_country_model_forecast()
                with col3:
                    st.write("Market Shares")
                    st.write(share_model)
                with col4:
                    st.write("Market Forecast")
                    st.write(forecast_model)
                with col5:
                    st.write("Original Size")
                    st.write(sql_country_model_size)
                with col6:
                    st.write("Original Forecast")
                    st.write(sql_country_model_forecast)
                with col7:
                    model_comparison_size = CountryModelEvaluation(db_cxcn).compare_country_model_size(share_model,sql_country_model_size)
                    st.write("Size Comparison")
                    st.write(model_comparison_size)
                with col8:
                    model_comparison_forecast = CountryModelEvaluation(db_cxcn).compare_country_model_forecast(forecast_model,sql_country_model_forecast)
                    st.write("Forecast Comparison")
                    st.write(model_comparison_forecast)




