
import streamlit as st
from myCountryModelPackages.CountryModelEvaluationTools import *
from myCountryModelPackages.MarketReportRetrieval import *
# from pages.Economic import db_cxcn


country_model_shares = st.session_state['share_country_model']
country_model_forecast = st.session_state['forecast_country_model']
db_cxcn = st.session_state['db_cxcn']
market_report = st.session_state['market_report']
base_year = st.session_state['base_year']

sql_market_data = MarketReportData(db_cxcn, market_report, base_year)
sql_country_model_size = sql_market_data.get_country_model_size()
sql_country_model_forecast = sql_market_data.get_country_model_forecast()

st.title("Country Model Analysis" )
st.write( str(base_year) + " --- " + market_report )

col1, col2,col3 = st.columns(3)
with col1:
    if 'share_country_model' in st.session_state:
        st.write("Country Model Market Shares")
        st.dataframe(country_model_shares)
with col2:
    if 'forecast_country_model' in st.session_state:
        st.write("Country Model Forecast")
        st.dataframe(st.session_state['forecast_country_model'])

with col3:
    if st.button('Run Comparison Analysis'):
        with col1:
            model_comparison_size = CountryModelEvaluation(db_cxcn).compare_country_model_size(country_model_shares, sql_country_model_size)
            st.write("Size Comparison")
            st.dataframe(model_comparison_size)
        with col2:
            model_comparison_forecast = CountryModelEvaluation(db_cxcn).compare_country_model_forecast(country_model_forecast ,sql_country_model_forecast)
            st.write("Forecast Comparison")
            st.dataframe(model_comparison_forecast)
        st.write("ran comparison")