import streamlit as st
from myCountryModelPackages.sqlTableRetrieve import *
from myCountryModelPackages.MarketReportRetrieval import *

st.set_page_config(layout="wide")

def display_page_info():
    st.markdown("<h1 style='text-align: center;'>Country Model Generator</h1>", unsafe_allow_html=True)
    text_message = "This application is designed to generated a Country Model from an published World Wide market Report.  \
                  Access to all the Country Model Economic Factors can be modified in several of the tabs on the left "
    st.markdown("<h3 style='font-size:16pt;'>" + text_message + "</h3>", unsafe_allow_html=True)

    st.title("Industry Concentration by Country Factors:")
    text_message = " Economic Industry Weights - this allows modification of the Industry weights "
    st.markdown("<h3 style='font-size:16pt;'>" + text_message + "</h3>", unsafe_allow_html=True)
    text_message = " Economic Automation Degree - this allows modification the Automation degree  "
    st.markdown("<h3 style='font-size:16pt;'>" + text_message + "</h3>", unsafe_allow_html=True)

    st.title("Country GDP Fraction:")
    text_message = \
        "This factor allows for adding a multiplier to the GDP for the entire country.  \
        This effectively allows you to reduce the GDP of a country if you think the GDP is not representative of the Automation in that country"
    st.markdown("<h3 style='font-size:16pt;'>" + text_message + "</h3>", unsafe_allow_html=True)

if 'initialized' not in st.session_state:
    st.session_state.initialized = False

if __name__ == '__main__':
    display_page_info()
    if not st.session_state.initialized:
        with st.spinner("Initializing..."):
            market_report_db_cxcn = DatabaseConnections().get_MiraLite_Connection()
            if 'db_cxcn_market_research' not in st.session_state:
                st.session_state.db_cxcn_market_research = market_report_db_cxcn
            if 'db_engine_publication' not in st.session_state:
                _db_engine = DatabaseConnections().get_MiraLite_engine()
                st.session_state.db_engine_publication = _db_engine
            if 'base_year' not in st.session_state:
                base_year_list = MarketReports(st.session_state.db_cxcn_market_research).get_base_year_list().sort_values(ascending=False)
                st.session_state.base_year = base_year_list.max()




