import streamlit as st
from myCountryModelPackages.MarketReportRetrieval import *
from myCountryModelPackages.ProductTechnologyGroup import *
from myCountryModelPackages.Economic_Research import *
from myCountryModelPackages.CM_SessionStates import global_session_states_initialize, global_session_states_key

st.set_page_config(layout="wide")

key_prefix = "PxT_"

selected_base_year = None

_GS_key_prefix = global_session_states_key()
global_session_states_initialize()

if f"{key_prefix}base_year_list" not in st.session_state:
    market_reports = MarketReports(st.session_state.db_cxcn_market_research)
    st.session_state[f"{key_prefix}base_year_list"] = \
        sorted(market_reports.get_base_year_list(),reverse=True)

if f"{key_prefix}msal_access_token"  not in st.session_state:
    graph_api = MSGraphTokens()
    access_token = graph_api.generate_access_token()
    st.session_state[f"{_GS_key_prefix}msal_access_token"] = access_token

if f"{key_prefix}ProductDescriptionTable" not in st.session_state:
    token = st.session_state[f"{_GS_key_prefix}msal_access_token"]
    st.session_state[f"{key_prefix}ProductDescriptionTable"] = ProductDescriptionTable(token)

if 'db_cxcn_market_research' not in st.session_state:
    _db_cxcn = DatabaseConnections().get_MiraLite_Connection()
    st.session_state.db_cxcn_market_research = _db_cxcn

if 'db_engine_publication' not in st.session_state:
    _db_engine = DatabaseConnections().get_MiraLite_engine()
    st.session_state.db_engine_publication = _db_engine

st.title("Product-X-Technology Group ")


products = \
    st.session_state[f"{key_prefix}ProductDescriptionTable"].market_study_list_via_graph()

technology_domains = \
    st.session_state[f"{key_prefix}ProductDescriptionTable"].technology_group_list_via_graph()

# product_list_col,  = st.columns(1) #technology_group_col = st.columns([1,1])

#with product_list_col:
#    st.dataframe(products, hide_index=True)

base_year_col, market_report_col, ad_technology_group_col, gdp_technology_group_col = st.columns([1,3,2,2])
erfr = EconomicResearchFactorsRanges()

with base_year_col:
    st.markdown("<h1 style='font-size:12pt;text-align: center;'>Base Year  </h1>",
                unsafe_allow_html=True)
    st.selectbox('', st.session_state[f"{key_prefix}base_year_list"],key = f"{key_prefix}base_year_select_value")

with market_report_col:
    st.markdown("<h1 style='font-size:12pt;text-align: center;'>Market Reports by Technology  </h1>", unsafe_allow_html=True)
    # st.dataframe(products, hide_index=True)
    styled_products = products.style.apply(
        lambda x: ['background-color: #DCECD1' if i % 2 == 0 else 'background-color: #ffffff' for i in range(len(x))],
        axis=0
    )
    st.dataframe(styled_products, use_container_width=True)

with ad_technology_group_col:
    st.markdown("<h1 style='font-size:12pt;text-align: center;'>Automation Degree Technology Groups for Base Year</h1>", unsafe_allow_html=True)
    tg_ad_list = erfr.getlist_automation_degree_research_technology_groups(st.session_state[f"{key_prefix}base_year_select_value"])
    tg_ad= pd.DataFrame(tg_ad_list, columns=["Automation Degree"])
    #st.dataframe(tg_ad)
    styled_tg_ad = tg_ad.style.apply(
        lambda x: ['background-color: #DCECD1' if i % 2 == 0 else 'background-color: #ffffff' for i in range(len(x))],
        axis=0
    )
    st.dataframe(styled_tg_ad, use_container_width=True)

with gdp_technology_group_col:
    st.markdown("<h1 style='font-size:12pt;text-align: center;'>Industrial GDP Technology Groups for Base Year</h1>",unsafe_allow_html=True)
    tg_gdp_list = erfr.getlist_cm_gdp_research_technology_groups(st.session_state[f"{key_prefix}base_year_select_value"])
    tg_gdp = pd.DataFrame(tg_gdp_list, columns=["Industrial GDP"])
    #st.dataframe(tg_gdp)
    styled_tg_gdp = tg_gdp.style.apply(
        lambda x: ['background-color: #DCECD1' if i % 2 == 0 else 'background-color: #ffffff' for i in range(len(x))],
        axis=0
    )
    st.dataframe(styled_tg_gdp, use_container_width=True)




