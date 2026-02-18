import streamlit as st

from myCountryModelPackages.MarketReportRetrieval import *
from myCountryModelPackages.Economic_Research import *
from myCountryModelPackages.CM_SessionStates import global_session_states_initialize

st.set_page_config(layout="wide")

key_prefix = "CMV_Versions_"

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


if f"{key_prefix}economic_base_year_range" not in st.session_state:
    economic_research = EconomicResearchFactorsRanges()
    economic_range = economic_research.get_economic_research_years()
    st.session_state[f"{key_prefix}economic_base_year_range"] = economic_range

if f"{key_prefix}restore_version_flag" not in st.session_state:
    st.session_state[f"{key_prefix}restore_version_flag"] = False

if f"{key_prefix}archive_version_flag" not in st.session_state:
    st.session_state[f"{key_prefix}archive_version_flag"] = False

if f"{key_prefix}archive_message" not in st.session_state:
    st.session_state[f"{key_prefix}archive_message"] = "Archiving has not started"

st.title("Version Management Country Model Economic Data")
text_message = "Partitioned Images of the Economic Data for selected base years are saved and tagged as a specific Version. "
st.markdown("<h3 style='font-size:14pt;'>" + text_message + "</h3>", unsafe_allow_html=True)
text_message = "Automation Intensity factors by Technology Group include: "
st.markdown("<h3 style='font-size:14pt;'>" + text_message + "</h3>", unsafe_allow_html=True)
st.markdown("""
<ul style='font-size:14pt;'>
    <li>Automation Degree  </li>
    <li>GDP Country Scalar </li>
</ul>
""", unsafe_allow_html=True)
text_message = "Country Economic Profile factors include: "
st.markdown("<h3 style='font-size:14pt;'>" + text_message + "</h3>", unsafe_allow_html=True)
st.markdown("""
<ul style='font-size:14pt;'>
    <li>Industry Fraction (How strong an Industry is in the Country) </li>
    <li>GDP Remainders (Used to estimate the GDP for unknown countries in the region)</li>
</ul>
<h3 style='font-size:12pt;'>Please note: Currently there is only one Technology Group and it is a default group for all reports.  </h3>
""", unsafe_allow_html=True)
st.markdown("<h3 style='font-size:16pt;'>" + " The Version Number is Auto Generated " + "</h3>", unsafe_allow_html=True)
st.markdown("<h3 style='font-size:16pt;'>" + " The Base Year ranges are available so a partition of the country model table can be archived. " + "</h3>", unsafe_allow_html=True)
version_year_first, version_year_last, version_comment_col, = st.columns([1,1,5])

with version_year_first:
    st.selectbox(
        'Select First Year',
        sorted(st.session_state[f"{key_prefix}economic_base_year_range"], reverse=True),
        key=f"{key_prefix}first_base_year_select_value"
    )
with version_year_last:
    st.selectbox(
        'Select Last Year',
        sorted(st.session_state[f"{key_prefix}economic_base_year_range"], reverse=True),
        key=f"{key_prefix}last_base_year_select_value"
    )
with version_comment_col:
    st.session_state[f"{key_prefix}archive_message"] = st.text_input("Add Version comment")

message_placeholder = st.empty()
message_placeholder.write( st.session_state[f"{key_prefix}archive_message"])

# Universal tables include the Industry Fraction X Country and the GDP Remainders
#  Version Management table is CMV_UEF_VersionManagement
def _start_universal_table_archive():
    first_year = st.session_state.get(f"{key_prefix}first_base_year_select_value")
    last_year = st.session_state.get(f"{key_prefix}last_base_year_select_value")
    comment = st.session_state.get(f"{key_prefix}archive_message")
    economic_research = EconomicResearchFactorsPublish()
    latest_version = economic_research.get_latest_version_uef_research()
    if latest_version == None:
        version = 1.0
    else:
        version = latest_version + 1
    with st.spinner(f"Archiving Version: {version} Country Economic Profile tables in progress... Please wait."):
        economic_research.archive_uef_economic_research(version, first_year, last_year, st.session_state[f"{key_prefix}archive_message"])

    st.session_state[f"{key_prefix}archive_message"] = f"Archiving started\nComment: {st.session_state[f"{key_prefix}archive_message"]} {latest_version}"

# Technology Group Tables include the Automation Degree and the GDP Multiplier by Country
#  Version Management table is CMV_VersionManagement
def _start_technology_table_archive():
    first_year = st.session_state.get(f"{key_prefix}first_base_year_select_value")
    last_year = st.session_state.get(f"{key_prefix}last_base_year_select_value")
    comment = st.session_state.get(f"{key_prefix}archive_message")
    economic_research = EconomicResearchFactorsPublish()
    latest_version = economic_research.get_latest_version_techgroup_economic_research()
    if latest_version == None:
        version = 1.0
    else:
        version = latest_version + 1
    with st.spinner(f"Archiving Version: {version} of the Technology Group Factor tables in progress... Please wait."):
        economic_research.archive_techgroup_economic_research(version, first_year, last_year, st.session_state[f"{key_prefix}archive_message"])

    st.session_state[f"{key_prefix}archive_message"] = f"Archiving started\nComment: {st.session_state[f"{key_prefix}archive_message"]} {latest_version}"


with st.sidebar:
    st.header("Version Management Actions by Table Groups")
    button_a = st.button("Country Economic Profile", key=f"{key_prefix}_start_refresh_button", on_click=_start_universal_table_archive)
    techGroup = st.button("Automation Intensity Factors", key=f"{key_prefix}_start_tg_refresh_button", on_click=_start_technology_table_archive)

