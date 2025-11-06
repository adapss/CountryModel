import streamlit as st

from myCountryModelPackages.MarketReportRetrieval import *
from myCountryModelPackages.Economic_Research import *
from myCountryModelPackages.CM_SessionStates import global_session_states_initialize

st.set_page_config(layout="wide")

key_prefix = "CMV_NewYear_"

def __get_all_technology_groups_list():
   pdt = (ProductDescriptionTable(st.session_state[f"{_GS_key_prefix}msal_access_token"]))
   tg_list = pdt.technology_group_list_via_graph()
   return tg_list

selected_base_year = None
# Two connections are provided so that there is flexibility to source the Worldwide Market study from a different
# database than where it is finally published
#   market_report_db_cxcn
#       Used to pull the published worldwide market report data which also includes the source of Country Known

global_session_states_initialize()
_GS_key_prefix = global_session_states_key()

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

if f"{key_prefix}tech_group_list" not in st.session_state:
    st.session_state[f"{key_prefix}tech_group_list"] = __get_all_technology_groups_list()


if f"{key_prefix}economic_base_year_range" not in st.session_state:
    economic_research = EconomicResearchFactorsRanges()
    economic_range = economic_research.get_economic_research_years()
    st.session_state[f"{key_prefix}economic_base_year_range"] = economic_range

if f"{key_prefix}copy_message" not in st.session_state:
    st.session_state[f"{key_prefix}copy_message"] = "Copy has not started"

def __get_technology_group_list(year:int):
   # pdt = (ProductDescriptionTable(st.session_state[f"{_GS_key_prefix}msal_access_token"]))
   # tg_list = pdt.technology_group_list_via_graph()
   erfr = EconomicResearchFactorsRanges()
   tg_list_gdp = erfr.getlist_cm_gdp_research_technology_groups(year)
   tg_list_ad = erfr.getlist_automation_degree_research_technology_groups(year)
   tg_list = set(tg_list_ad) & set(tg_list_gdp)
   return tg_list



all_technology_group_list = __get_all_technology_groups_list()

st.title("Copy Technology Group Tables Year-Copy to Year-Destination  - Country Model Economic Data")
text_message = " Country Model Economic Data for Technology Group Parameters created/copyied by year and technology group combinations"
st.markdown("<h3 style='font-size:12pt;'>" + text_message + "</h3>", unsafe_allow_html=True)
text_message = " The two tables that are copied include:"
st.markdown("<h3 style='font-size:12pt;'>" + text_message + "</h3>", unsafe_allow_html=True)
text_message = "Automation Degree: "
st.markdown("<h3 style='font-size:12pt;'>" + text_message + "</h3>", unsafe_allow_html=True)
text_message = "Automation GDP Scalar by Country "
st.markdown("<h3 style='font-size:12pt;'>" + text_message + "</h3>", unsafe_allow_html=True)

copy_year_col, copy_tech_group_col, dest_year_col , dest_tech_group_col = st.columns([1,3,1,3])

with copy_year_col:
    st.selectbox(
        'Copy Year',
        sorted(st.session_state[f"{key_prefix}economic_base_year_range"], reverse=True),
        key=f"{key_prefix}copy_base_year_select_value"
    )
with copy_tech_group_col:
    # _test = st.session_state[f"{key_prefix}tg_select_value"]
    technology_group_list = __get_technology_group_list(st.session_state.get(f"{key_prefix}copy_base_year_select_value"))
    st.selectbox('Technology Group', technology_group_list,
                 key=f"{key_prefix}tg_select_copy_value")
with dest_year_col:
    st.selectbox(
        'Select Destination Year',
        sorted(st.session_state[f"{key_prefix}base_year_list"], reverse=True),
        key=f"{key_prefix}destination_base_year_select_value"
    )
with dest_tech_group_col:
    # _test = st.session_state[f"{key_prefix}tg_select_value"]
    st.selectbox('Technology Group',  st.session_state[f"{key_prefix}tech_group_list"],
                 key=f"{key_prefix}destination_tg_select_value")

message_placeholder = st.empty()
message_placeholder.write( st.session_state[f"{key_prefix}copy_message"])

# Technology Group Tables include the Automation Degree and the GDP Multiplier by Country
#  Version Management table is CMV_VersionManagement
def _start_copy():
    pdt = ProductDescriptionTable(st.session_state[f"{_GS_key_prefix}msal_access_token"])
    copy_year = st.session_state.get(f"{key_prefix}copy_base_year_select_value")
    copy_tg = st.session_state.get(f"{key_prefix}tg_select_copy_value")
    copy_tg_id = pdt.lookup_technology_group_id(copy_tg)
    destination_year = st.session_state.get(f"{key_prefix}destination_base_year_select_value")
    destination_tg = st.session_state.get(f"{key_prefix}destination_tg_select_value")
    destination_tg_id = pdt.lookup_technology_group_id(destination_tg)

    economic_research = EconomicResearchFactorsPublish()
    with st.spinner(f"Copying: {copy_year} to  {destination_year} Country Technology Group Model tables in progress... Please wait."):
        st.write("copy of technology Groups")
        economic_research.copy_technology_group_economic_research(copy_year, copy_tg_id, destination_year, destination_tg_id)

with st.sidebar:
    st.header("Copy Technology Group Model Parameters")
    button_a = st.button("Start Copy", key=f"{key_prefix}_start_copy_button", on_click=_start_copy)


