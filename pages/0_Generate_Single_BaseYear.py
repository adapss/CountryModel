import streamlit as st
from app.myCountryModelPackages.CountryModel_Generation import *
from app.myCountryModelPackages.sqlTableRetrieve import *
from app.myCountryModelPackages.MarketReportRetrieval import *
from app.myCountryModelPackages.CM_SessionStates import initialize_global_session_states

st.set_page_config(layout="wide")

key_prefix = "SS_Generator_"

selected_base_year = None
# Two connections are provided so that there is flexibility to source the Worldwide Market study from a different
# database than where it is finally published
#   market_report_db_cxcn
#       Used to pull the published worldwide market report data which also includes the source of Country Known

initialize_global_session_states()

if 'db_cxcn_market_research' not in st.session_state:
    _db_cxcn = DatabaseConnections().get_MiraLite_Connection()
    st.session_state.db_cxcn_market_research = _db_cxcn

if 'db_engine_publication' not in st.session_state:
    _db_engine = DatabaseConnections().get_MiraLite_engine()
    st.session_state.db_engine_publication = _db_engine

#if f"{key_prefix}selected_base_year" not in st.session_state:
#    st.session_state[f"{key_prefix}selected_base_year"] = st.session_state.base_year

if f"{key_prefix}base_year_list" not in st.session_state:
    st.session_state[f"{key_prefix}base_year_list"] = None
if f"{key_prefix}report_list" not in st.session_state:
    st.session_state[f"{key_prefix}report_list"] = None
if f"{key_prefix}selected_base_year_prev" not in st.session_state:
    st.session_state[f"{key_prefix}selected_base_year_prev"] = st.session_state.base_year

if f"{key_prefix}base_year_select_value" not in st.session_state:
    st.session_state[f"{key_prefix}base_year_select_value"] = st.session_state.base_year
 #   st.session_state[f"{key_prefix}selected_base_year"] = st.session_state.base_year
    st.session_state[f"{key_prefix}selected_base_year_prev"]  = None

if st.session_state[f"{key_prefix}selected_base_year_prev"] is None or st.session_state[f"{key_prefix}base_year_select_value"] != st.session_state[f"{key_prefix}selected_base_year_prev"]:
    market_reports = MarketReports(st.session_state.db_cxcn_market_research)
    report_list = market_reports.get_report_list()
    st.session_state[f"{key_prefix}report_list"] = report_list.sort_values(by='Study', ascending=True)
    st.session_state[f"{key_prefix}base_year_list"] = \
        sorted(market_reports.get_base_year_list(),reverse=True)
    st.session_state[f"{key_prefix}selected_base_year_prev"] = st.session_state[f"{key_prefix}base_year_select_value"]

if f"{key_prefix}market_report_select_value" not in st.session_state:
    filtered_reports = report_list[report_list['BaseYear'] == st.session_state[f"{key_prefix}base_year_select_value"]]
    filtered_reports = filtered_reports.drop(['BaseYear'], axis=1)
    default_report = filtered_reports['Study'].min()
    st.session_state[f"{key_prefix}market_report_select_value"] = default_report

if 'share_country_model' not in st.session_state:
    st.session_state['share_country_model'] = None
if 'forecast_country_model' not in st.session_state:
    st.session_state['forecast_country_model'] = None

if f"{key_prefix}generate_model_flag"not in st.session_state:
    st.session_state[f"{key_prefix}generate_model_flag"] = False

st.title("Country Model Generator")
text_message = "This application is designed to generate a Country Model from an previously published World Wide market Report. \
        So the Worldwide market data is necessary, however it is recommended that you load the Country Known data as well.  \
        Country Known data should be loaded in tandem with your worldwide report so that data is consistent."
st.markdown("<h3 style='font-size:16pt;'>" + text_message + "</h3>", unsafe_allow_html=True)

base_year_col, market_report_col, generate_report_col = st.columns(3)

def click_button():
    st.session_state[f"{key_prefix}generate_model_flag"] = True  #not st.session_state.button[f"{key_prefix}generate_model_button"]

with base_year_col:
    st.selectbox('Select Base Year', st.session_state[f"{key_prefix}base_year_list"],key = f"{key_prefix}base_year_select_value")
    st.session_state['base_year'] =  st.session_state[f"{key_prefix}base_year_select_value"]

with market_report_col:
    report_list = st.session_state[f"{key_prefix}report_list"]
    filtered_df = report_list[report_list['BaseYear'] == st.session_state[f"{key_prefix}base_year_select_value"]]
    filtered_df = filtered_df.drop(['BaseYear'], axis=1)
    st.selectbox('Select Report', filtered_df['Study'].tolist() ,index = 0, key = f"{key_prefix}market_report_select_value")
    st.session_state['market_report'] = st.session_state[f"{key_prefix}market_report_select_value"]
with generate_report_col:
    st.button('Start Model Generation for Selected Report',
        on_click=click_button,
        key = f"{key_prefix}generate_model_button")

col1, col2,col3 = st.columns(3)

with (col1):
    if st.session_state[f"{key_prefix}generate_model_flag"]:
#with st.spinner("Generating Country Models and Publishing to the database..."):
       st.session_state[f"{key_prefix}generate_model_flag"] = False
       country_model = \
           Country_Model_Generation(st.session_state.db_cxcn_market_research,
                                    st.session_state[f"{key_prefix}market_report_select_value"],
                                    st.session_state[f"{key_prefix}base_year_select_value"]
                                    )
       valid_report_message = country_model.validate_world_wide_report()
       if  valid_report_message == "":
           with st.spinner("Generating Country Model and Publishing to the database..."):
               country_share_model = country_model.generate_market_shares()
               st.session_state['share_country_model'] = country_share_model

               country_forecast_model = country_model.generate_forecast()
               st.session_state['forecast_country_model'] = country_forecast_model
               publish_model = \
                   Country_Model_Publish(st.session_state.db_engine_publication,
                                         st.session_state[f"{key_prefix}market_report_select_value"],
                                         st.session_state[f"{key_prefix}base_year_select_value"],
                                         country_share_model, country_forecast_model)
       else:
           #st.write(valid_report_message)
           # Create a single column layout
           warning_col = st.columns(1)[0]  # Access the first (and only) column
           with col2:
               CountryModelRemove(st.session_state.db_engine_publication,
                                 st.session_state[f"{key_prefix}market_report_select_value"],
                                 st.session_state[f"{key_prefix}base_year_select_value"]).delete_country_model()
               warning_message = 'Unable to Generate Country Model due to the following reasons: \n' + valid_report_message
               st.markdown(
                   "<div style='color: #856404; background-color: #fff3cd; padding: 10px; border-radius: 5px; font-size: 20px; white-space: pre-wrap;'>" +
                   warning_message +
                   "</div>",
                   unsafe_allow_html=True
               )
               st.stop()

#sql_market_data = MarketReportData(DatabaseConnections().get_MiraLite_Connection(), selected_report, st.session_state[f"{key_prefix}selected_base_year"])
       #sql_country_model_size = sql_market_data.get_country_model_size()
       #sql_country_model_forecast = sql_market_data.get_country_model_forecast()
       #country_model_comparison = CountryModelComparisonTest(country_share_model, sql_country_model_size, country_forecast_model, sql_country_model_forecast)
       with col1:
           st.write("Market Shares")
           st.write(country_share_model)
           st.spinner("Publishing Country Market Share Model to the Database")
           publish_model.publish_market_shares()

       with col2:
           st.write("Market Forecast")
           st.write(country_forecast_model)
           st.spinner("Publishing Country Model Forecast to the Database")
           publish_model.publish_market_forecast()


