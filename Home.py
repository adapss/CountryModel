import os
import time
import pandas as pd
import pyodbc
import streamlit as st
st.set_page_config(layout="wide")

from myCountryModelPackages.CountryModel_Generation import *
from myCountryModelPackages.MarketReportRetrieval import MarketReports
from myCountryModelPackages.sqlTableRetrieve import *
from myCountryModelPackages.MarketReportRetrieval import *
from myCountryModelPackages.CountryModelEvaluationTools import *

selected_base_year = None

if __name__ == '__main__':
    st.title("Country Model Generator")
    st.write("This application is designed to generated a Country Model from an previously published World Wide market Report.")
    db_cxcn = sqlConnection().get_connection()
    st.session_state['db_cxcn'] = db_cxcn

    market_reports = MarketReports(db_cxcn)
    report_list = market_reports.get_report_list()
    report_list = report_list.sort_values(by='Study', ascending=True)
    base_year_list = market_reports.get_base_year_list().sort_values(ascending=False)
    # Add buttons for user actions
    col1, col2, col3 = st.columns(3)

    if 'button' not in st.session_state:
        st.session_state.button = False
    def click_button():
        st.session_state.button = not st.session_state.button

    # Display the Report List
    with col3:
        # st.button('Create Country Model') #st.caption("Press the button to generate Country Model tables")
        st.button('Start Model Generation for Selected Report', on_click=click_button)
    with col1:
        # st.write('Market Report Selection:')
        #selected_base_year = st.selectbox(f'Select Base Year', base_year_list)
        selected_base_year = st.selectbox('', base_year_list)
        st.session_state['base_year'] = selected_base_year
    with col2:
        filtered_df = report_list[report_list['BaseYear'] == selected_base_year]
        # selected_report = st.multiselect('Select report:', filtered_df['Study'])
        # selected_report = st.selectbox('Select report:', filtered_df['Study'])
        selected_report = st.selectbox('', filtered_df['Study'])
        st.session_state['market_report'] = selected_report

    # Add buttons for user actions
    col1, col2,col3 = st.columns(3)
#    with col1:
#        st.caption("Press the button to generate Country Model tables")
#    col3, col4, col5, col6,col7,col8 = st.columns(6)
    with col1:
        if st.session_state.button: #st.button('Create Country Model'):
           with st.spinner("Loading data..."):
                country_model = Country_Model_Generation( db_cxcn, selected_report, selected_base_year)
                share_model = country_model.generate_market_shares()
                st.session_state['share_country_model'] =share_model

                forecast_model = country_model.generate_forecast()
                st.session_state['forecast_country_model'] = forecast_model

                sql_market_data = MarketReportData(db_cxcn, selected_report, selected_base_year)
                sql_country_model_size = sql_market_data.get_country_model_size()
                sql_country_model_forecast = sql_market_data.get_country_model_forecast()
                with col1:
                    st.write("Market Shares")
                    st.write(share_model)
                with col2:
                    st.write("Market Forecast")
                    st.write(forecast_model)





