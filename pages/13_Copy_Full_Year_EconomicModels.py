import streamlit as st

from myCountryModelPackages.MarketReportRetrieval import *
from myCountryModelPackages.Economic_Research import *
from myCountryModelPackages.CM_SessionStates import global_session_states_initialize

st.set_page_config(layout="wide")

key_prefix = "CMV_NewYear_"

selected_base_year = None
# Two connections are provided so that there is flexibility to source the Worldwide Market study from a different
# database than where it is finally published
#   market_report_db_cxcn
#       Used to pull the published worldwide market report data which also includes the source of Country Known

global_session_states_initialize()

if 'db_cxcn_market_research' not in st.session_state:
    _db_cxcn = DatabaseConnections().get_MiraLite_Connection()
    st.session_state.db_cxcn_market_research = _db_cxcn

if 'db_engine_publication' not in st.session_state:
    _db_engine = DatabaseConnections().get_MiraLite_engine()
    st.session_state.db_engine_publication = _db_engine

if f"{key_prefix}base_year_list" not in st.session_state:
    market_reports = MarketReports(st.session_state.db_cxcn_market_research)
    st.session_state[f"{key_prefix}base_year_list"] = \
            sorted(market_reports.get_base_year_list(),reverse=True)

if f"{key_prefix}economic_base_year_range" not in st.session_state:
    economic_research = EconomicResearchFactorsRanges()
    economic_range = economic_research.get_economic_research_years()
    st.session_state[f"{key_prefix}economic_base_year_range"] = economic_range

if f"{key_prefix}copy_message" not in st.session_state:
    st.session_state[f"{key_prefix}copy_message"] = "Copy has not started"

st.title("Copy Base Year or Create New - Country Model Economic Data")
text_message = " A new base year for country model economic data can be created by copying from an prior year"
st.markdown("<h3 style='font-size:12pt;'>" + text_message + "</h3>", unsafe_allow_html=True)
text_message = " This can also be used to copy one year to another."
st.markdown("<h3 style='font-size:12pt;'>" + text_message + "</h3>", unsafe_allow_html=True)
text_message = "Automation Intensity factors by Technology Group include: "
st.markdown("<h3 style='font-size:12pt;'>" + text_message + "</h3>", unsafe_allow_html=True)
text_message = "Country Economic Profile factors include: "
st.markdown("<h3 style='font-size:12pt;'>" + text_message + "</h3>", unsafe_allow_html=True)

copy_year, new_year = st.columns([1,1])

with copy_year:
    st.selectbox(
        'Copy Year',
        sorted(st.session_state[f"{key_prefix}economic_base_year_range"], reverse=True),
        key=f"{key_prefix}copy_base_year_select_value"
    )
with new_year:
    st.selectbox(
        'Select Destination Year',
        sorted(st.session_state[f"{key_prefix}base_year_list"], reverse=True),
        key=f"{key_prefix}destination_base_year_select_value"
    )

message_placeholder = st.empty()
message_placeholder.write( st.session_state[f"{key_prefix}copy_message"])

# Technology Group Tables include the Automation Degree and the GDP Multiplier by Country
#  Version Management table is CMV_VersionManagement
def _start_new_year_copy():
    copy_year = st.session_state.get(f"{key_prefix}copy_base_year_select_value")
    destination_year = st.session_state.get(f"{key_prefix}destination_base_year_select_value")
    economic_research = EconomicResearchFactorsPublish()
    with st.spinner(f"Copying: {copy_year} to  {destination_year} Country Model tables in progress... Please wait."):
        economic_research.copy_economic_research(copy_year, destination_year)



with st.sidebar:
    st.header("New Country Model Year")
    button_a = st.button("Start Copy", key=f"{key_prefix}_start_copy_button", on_click=_start_new_year_copy)


