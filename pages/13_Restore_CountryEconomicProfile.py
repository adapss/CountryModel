import streamlit as st
from myCountryModelPackages.CountryModel_Generation import *
from myCountryModelPackages.sqlTableRetrieve import *
from myCountryModelPackages.MarketReportRetrieval import *
from myCountryModelPackages.Economic_Research import *
from myCountryModelPackages.CM_SessionStates import global_session_states_initialize

st.set_page_config(layout="wide")

key_prefix = "CMV_Restore_UEF"

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

if f"{key_prefix}restore_version_flag" not in st.session_state:
    st.session_state[f"{key_prefix}restore_version_flag"] = False

if f"{key_prefix}archive_version_flag" not in st.session_state:
    st.session_state[f"{key_prefix}archive_version_flag"] = False

if f"{key_prefix}restore_message" not in st.session_state:
    st.session_state[f"{key_prefix}restore_message"] = "Restore Progress"

st.title("Restore - Country Economic Profile Factors are Common Across all Reports")
text_message = "Country Economic Profile factors represent economics of each country which is consistent across all Technology Groups and is market report independent. "
st.markdown("<h3 style='font-size:16pt;'>" + text_message + "</h3>", unsafe_allow_html=True)
st.markdown("""
<ul style='font-size:14pt;'>
    <li>Industry Fraction (How strong an Industry is in the Country) </li>
    <li>GDP Remainders (Used to estimate the GDP for unknown countries in the region)</li>
</ul>
<h3 style='font-size:12pt;'>Please note: Currently there is only one Technology Group and it is a default group for all reports.  </h3>
""", unsafe_allow_html=True)
version_select_col, display_version_history_col = st.columns([1,5])

economic_research = EconomicResearchFactorsPublish()
version_history = economic_research.get_uef_version_table_economic_research()
version_display = version_history.drop('VersionKey', axis=1)

with display_version_history_col:
    st.dataframe(version_display, hide_index=True)

with version_select_col:
    selected_version = st.selectbox(
        "Select a Version to Restore",
        options=version_history['Version'].tolist()
    )

message_placeholder = st.empty()
message_placeholder.write( st.session_state[f"{key_prefix}restore_message"])

def _universal_restore_version():
    with message_placeholder:
        filtered_row = version_history[version_history['Version'] == selected_version]
        if not filtered_row.empty:
            version_key = int(filtered_row['VersionKey'].iloc[0])
            first_year = int(filtered_row['YearFirst'].iloc[0])
            last_year = int(filtered_row['YearLast'].iloc[0])
            st.write(f"Primary Key for Version {version_key}: {version_key}")
            with st.spinner(f"Restoring Version: {selected_version} of the Country Economic Profile tables  in progress... Please wait."):
                economic_research.restore_version_uef_economic_research(version_key, first_year, last_year)
        else:
            st.warning("No matching version found.")
    # st.write(f"Version: {selected_version} : Restore started")
with st.sidebar:
    st.header("Version Management Actions")
    button_b = st.button("Country Economic Profile", key=f"{key_prefix}_universal_restore_button", on_click=_universal_restore_version)
