import os
import time
import pandas as pd
import pyodbc
import streamlit as st

#from myCountryModelPackages.sqlTableRetrieve import sql_MiraWIP_Connection

st.set_page_config(layout="wide")

from myCountryModelPackages.CountryModel_Generation import *
from myCountryModelPackages.MarketReportRetrieval import MarketReports
from myCountryModelPackages.sqlTableRetrieve import *
from myCountryModelPackages.MarketReportRetrieval import *
from myCountryModelPackages.CountryModelEvaluationTools import *



if __name__ == '__main__':
    st.markdown("<h1 style='text-align: center;'>Country Model Generator</h1>", unsafe_allow_html=True)
    text_message ="This application is designed to generated a Country Model from an published World Wide market Report.  \
              Access to all the Country Model Economic Factors can be modified in several of the tabs on the left "
    st.markdown("<h3 style='font-size:16pt;'>" + text_message + "</h3>", unsafe_allow_html=True)

    st.title("Industry Concentration by Country Factors:")
    text_message =" Economic Industry Weights - this allows modification of the Industry weights "
    st.markdown("<h3 style='font-size:16pt;'>" + text_message + "</h3>", unsafe_allow_html=True)
    text_message =" Economic Automation Degree - this allows modification the Automation degree  "
    st.markdown("<h3 style='font-size:16pt;'>" + text_message + "</h3>", unsafe_allow_html=True)

    st.title("Country GDP Fraction:")
    text_message = \
             "This factor allows for adding a multiplier to the GDP for the entire country.  \
             This effectively allows you to reduce the GDP of a country if you think the GDP is not representative of the Automation in that country"
    st.markdown("<h3 style='font-size:16pt;'>" + text_message + "</h3>", unsafe_allow_html=True)

    if 'base_year' not in st.session_state:
        market_report_db_cxcn = DatabaseConnections().get_MiraLite_Connection()
        publication_db_engine = DatabaseConnections().get_MiraLite_engine()
        base_year_list = MarketReports(market_report_db_cxcn).get_base_year_list().sort_values(ascending=False)
        max_base_year = base_year_list.max()
        st.session_state.base_year = max_base_year


