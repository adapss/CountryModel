import os
import time
import pandas as pd
import pyodbc
import streamlit as st

from myCountryModelPackages.sqlTableRetrieve import sql_MiraWIP_Connection

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

    # Two connections are provided so that there is flexibility to source the Worldwide Market study from a different
    # database than where it is finally published
    #   market_report_db_cxcn
    #       Used to pull the published worldwide market report data which also includes the source of CountryKnowns
    market_report_db_cxcn = DatabaseConnections().get_MiraLite_Connection()
    #   publication_db_engine
    #       database to publish the generated models.  need the sqlAlchemey engine connection
    #       different type of connection required for pushing data in to the database with sqlAlchemy
    #       also it gives flexibility to put it in a different database than the source reports.
    publication_db_engine = DatabaseConnections().get_MiraLite_engine()

    st.session_state['db_cxcn_market_research'] = market_report_db_cxcn

    market_reports = MarketReports(market_report_db_cxcn)
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
                country_model = Country_Model_Generation(market_report_db_cxcn, selected_report, selected_base_year)
                country_share_model = country_model.generate_market_shares()
                st.session_state['share_country_model'] =country_share_model

                country_forecast_model = country_model.generate_forecast()
                st.session_state['forecast_country_model'] = country_forecast_model
                publish_model = Country_Model_Publish(publication_db_engine, selected_report, selected_base_year, country_share_model, country_forecast_model)

                sql_market_data = MarketReportData(DatabaseConnections().get_MiraLite_Connection(), selected_report, selected_base_year)
                sql_country_model_size = sql_market_data.get_country_model_size()
                sql_country_model_forecast = sql_market_data.get_country_model_forecast()

                country_model_comparison = CountryModelComparisonTest(country_share_model, sql_country_model_size, country_forecast_model, sql_country_model_forecast)
                with col1:
                    st.write("Market Shares")
                   # country_model_share_diff = country_model_comparison.market_share_comparison()
                    st.write(country_share_model)
                    publish_model.publish_market_shares()

                with col2:
                    st.write("Market Forecast")
                   # country_model_forecast_diff = country_model_comparison.market_forecast_comparison()
                    st.write(country_forecast_model)
                    publish_model.publish_market_forecast()





