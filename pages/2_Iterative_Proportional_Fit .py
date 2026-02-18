import streamlit as st
from myCountryModelPackages.CountryModel_RAS import *
from myCountryModelPackages.CountryModel_Generation import *
from myCountryModelPackages.sqlTableRetrieve import *
from myCountryModelPackages.MarketReportRetrieval import *
from myCountryModelPackages.ProductTechnologyGroup import *
from myCountryModelPackages.CM_SessionStates import global_session_states_initialize, global_session_states_key

st.set_page_config(layout="wide")

key_prefix = "SS_Generator_"
_GS_key_prefix = global_session_states_key()
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

if f"{key_prefix}msal_access_token"  not in st.session_state:
    graph_api = MSGraphTokens()
    access_token = graph_api.generate_access_token()
    st.session_state[f"{_GS_key_prefix}msal_access_token"] = access_token

if f"{key_prefix}ProductDescriptionTable" not in st.session_state:
    token = st.session_state[f"{_GS_key_prefix}msal_access_token"]
    st.session_state[f"{key_prefix}ProductDescriptionTable"] = ProductDescriptionTable(token)

if f"{key_prefix}base_year_list" not in st.session_state:
    st.session_state[f"{key_prefix}base_year_list"] = None

if f"{key_prefix}report_list" not in st.session_state:
    st.session_state[f"{key_prefix}report_list"] = None

if f"{key_prefix}selected_base_year_prev" not in st.session_state:
    st.session_state[f"{key_prefix}selected_base_year_prev"] = st.session_state.base_year

if f"{key_prefix}base_year_select_value" not in st.session_state:
    st.session_state[f"{key_prefix}base_year_select_value"] = st.session_state.base_year
    st.session_state[f"{key_prefix}selected_base_year_prev"]  = None

if f"{key_prefix}technology_id_value" not in st.session_state:
    st.session_state[f"{key_prefix}technology_id"] = None

if st.session_state[f"{key_prefix}selected_base_year_prev"] is None or st.session_state[f"{key_prefix}base_year_select_value"] != st.session_state[f"{key_prefix}selected_base_year_prev"]:
    market_reports = MarketReports(st.session_state.db_cxcn_market_research)
    report_list = market_reports.get_country_model_report_list()
    st.session_state[f"{key_prefix}report_list"] = report_list.sort_values(by='Study', ascending=True)
    st.session_state[f"{key_prefix}base_year_list"] = \
        sorted(market_reports.get_country_model_base_year_list(),reverse=True)
    st.session_state[f"{key_prefix}selected_base_year_prev"] = st.session_state[f"{key_prefix}base_year_select_value"]

if f"{key_prefix}market_report_select_value" not in st.session_state:
    filtered_reports = report_list[report_list['BaseYear'] == st.session_state[f"{key_prefix}base_year_select_value"]]
    filtered_reports = filtered_reports.drop(['BaseYear'], axis=1)
    default_report = filtered_reports['Study'].min()
    st.session_state[f"{key_prefix}market_report_select_value"] = default_report

if f"{key_prefix}share_country_model" not in st.session_state:
    st.session_state[f"{key_prefix}share_country_model"] = None
if f"{key_prefix}forecast_country_model" not in st.session_state:
    st.session_state[f"{key_prefix}forecast_country_model"] = None

if f"{key_prefix}align_model_flag"not in st.session_state:
    st.session_state[f"{key_prefix}align_model_flag"] = False

#st.title("Iterative Proportional Fitting (IPF) Country Model Alignment <br> Industry & Region Alignment with Worldwide Market Report")

st.title("Country Model Alignment Using - Iterative Proportional Fitting Algorithm ")
st.subheader("Industry & Region Alignment with Worldwide Market Report")

text_message = ("This application applies the Iterative Proportional Fitting (IPF) algorithm (also referred as RAS) to enforce Region - Industry - Company alignment of the Country Model with \
        the published World Wide market Report. The Country Model that has been generated only has Regions aligned. \
        The IPF algorithm will align the Industries and Regions with the world wide report. \
        The algorithm first aligns the Country Market Size table which will alter the Industry by Regions.  Then the Country Market Forecast is aligned with the  \
        Worldwide report, however we use the results of market size alignment to ensure the Regional Industries are aligned with the Base Year corrections. \
        Throughout the forecast, the Industry by Regions will be adjusted without any constraints applied ")

text_reference = ("Reference: Fienberg (1970): Multi‑way convergence")

st.markdown(
    f"<h3 style='font-size:12pt;'>{text_message}</h3>",
    unsafe_allow_html=True
)
st.markdown(
    f"<h3 style='font-size:12pt; font-weight:bold;'><b>{text_reference}</b></h3>",
    unsafe_allow_html=True
)
base_year_col, market_report_col, technology_group_col = st.columns([1,2,4])

def _start_model_RAS_alignment():
    st.session_state[f"{key_prefix}align_model_flag"] = True  #not st.session_state.button[f"{key_prefix}generate_model_button"]

with base_year_col:
    st.markdown("<h1 style='font-size:12pt;text-align: center;'>Base Year</h1>", unsafe_allow_html=True)
    st.selectbox('Select Base Year', st.session_state[f"{key_prefix}base_year_list"],key = f"{key_prefix}base_year_select_value")
    st.session_state['base_year'] =  st.session_state[f"{key_prefix}base_year_select_value"]

with market_report_col:
    st.markdown("<h1 style='font-size:12pt;text-align: center;'>Market Report</h1>", unsafe_allow_html=True)
    report_list = st.session_state[f"{key_prefix}report_list"]
    filtered_df = report_list[report_list['BaseYear'] == st.session_state[f"{key_prefix}base_year_select_value"]]
    filtered_df = filtered_df.drop(['BaseYear'], axis=1)
    st.selectbox('Select Report', filtered_df['Study'].tolist() ,index = 0, key = f"{key_prefix}market_report_select_value")
    st.session_state['market_report'] = st.session_state[f"{key_prefix}market_report_select_value"]

with technology_group_col:
    pdt = st.session_state[f"{key_prefix}ProductDescriptionTable"]
    technology_group_name,st.session_state[f"{key_prefix}technology_id"]  = pdt.get_market_study_technology_group(st.session_state['market_report'])
    st.markdown("<h1 style='font-size:12pt;text-align: center;'>Information Panel <br></h1>", unsafe_allow_html=True)
    st.markdown(f"""
    <div style="border:1px solid #ccc; padding:4px; border-radius:5px; background-color:#f9f9f9; width:500px;">
       <strong>Technology Group:   </strong> {technology_group_name}, <br><strong>Technology Group ID:</strong> {st.session_state[f"{key_prefix}technology_id"]}
    </div>
    """, unsafe_allow_html=True)
    mrd = MarketReportData(st.session_state.db_cxcn_market_research,st.session_state['market_report'],st.session_state[f"{key_prefix}base_year_select_value"])
    country_known_info = mrd.get_country_known_sizes_list()
    economic_research= EconomicResearchFactorsRanges()
    valid_countries = economic_research.getlist_countries_from_industry_weights_research_all_regions(st.session_state[f"{key_prefix}base_year_select_value"])

    if country_known_info is not None:
        country_known_info = country_known_info[country_known_info["Country"].isin(valid_countries)]

    text_message = "<br>The list of countries in the table are designated as KNOWN by the analyst. <br> \
        Only, those countries which are available in the Economic Model are included.   \
        The market size of these countries will NOT be modeled.  However the industry size will be distributed in each country according to the model. <br>"
    st.markdown("<h3 style='font-size:14pt;'>" + text_message + "</h3>", unsafe_allow_html=True)
    st.markdown("""
    <style>
    [data-testid="stDataFrame"] div[data-testid="stDataFrameCell"] {
        font-size: 11px;       /* Smaller font */
        padding: 2px 2px;      /* Reduce cell padding */
    }
    [data-testid="stDataFrame"] div[data-testid="stDataFrameRow"] {
        height: 15px;          /* Reduce row height */
    }
    </style>
    """, unsafe_allow_html=True)

    if country_known_info is None:
        # Option A: show a friendly placeholder (Streamlit example)
        st.info("No country information available for the selected base year.")
        styled_df = None  # or skip creating it
    else:
        styled_df = country_known_info.style.apply(
            lambda col: [
                'background-color: #DCECD1' if i % 2 == 0 else 'background-color: #ffffff'
                for i in range(len(col))
            ],
            axis=0
        )

    st.dataframe(styled_df, use_container_width=True)

with st.sidebar:
    st.header("RAS Align")
    st.button('Start',
              on_click=_start_model_RAS_alignment,
              key=f"{key_prefix}generate_model_button")

col1, col2,col3 = st.columns(3)

with (col1):
    if st.session_state[f"{key_prefix}align_model_flag"]:
       st.session_state[f"{key_prefix}align_model_flag"] = False
       base_year_value = st.session_state[f"{key_prefix}base_year_select_value"]


       valid_report_message = ""   # placeholder to; will check whether a country model exists for this year and report

       if  valid_report_message == "":
           with st.spinner("IPF Alignment of Country Model ..."):
               IPF_Correction = Country_Model_Forecast_RAS_Balancing(
                   st.session_state.db_cxcn_market_research,
                   st.session_state[f"{key_prefix}market_report_select_value"],
                   base_year_value)

               country_model_size_aligned = IPF_Correction.align_market_size_by_company_region_and_industry_with_worldwide()
               st.session_state[f"{key_prefix}share_country_model"] = country_model_size_aligned

               industry_by_region_base_year= (
                    country_model_size_aligned[country_model_size_aligned['BaseYear'] == base_year_value]
                    .groupby(['BaseYear', 'Region', 'Industry'])['Size']
                    .sum()
                    .to_frame('Target_Value')
                    .reset_index()
                    .rename(columns={'BaseYear': 'Year'})
               )

               country_model_forecast_aligned = IPF_Correction.align_forecast_by_region_and_industry(industry_by_region_base_year)
               st.session_state[f"{key_prefix}forecast_country_model"] = country_model_forecast_aligned


       else:
           warning_col = st.columns(1)[0]
           with col2:
               st.markdown(f"""
                   <h3 style='font-size:14pt;'>Unable to Generate Country Model due to the following reasons:</h3>
                   <ul style='font-size:12pt;'>
                       <li>Market Report: {valid_report_message}</li>
                    </ul>
                   """, unsafe_allow_html=True)
               st.stop()

       with col1:
           st.write("Market Shares - IPF Aligned")
           st.write(country_model_size_aligned)
           tol_totals, tol_region, tol_industry = IPF_Correction.verification_market_size_by_company_region_and_industry_with_worldwide()
           st.write("Company Totals out of Tolerance")
           st.write(tol_totals)
           st.write("Company Industry out of Tolerance")
           st.write(tol_region)
           st.write("Company Region out of Tolerance")
           st.write(tol_industry)
       with col2:
           st.write("Country Model Forecast - IPF Aligned")
           st.write(country_model_forecast_aligned)
           tol_base_year_industry_x_region, tol_industry_total_x_year, tol_region_total_x_year = IPF_Correction.verification_market_forecast_by_region_and_industry_with_worldwide()
           st.write("Industry Totals by Region on Base Year out of Tolerance")
           st.write(tol_base_year_industry_x_region)
           st.write("Industry Totals by Year out of Tolerance")
           st.write(tol_industry_total_x_year)
           st.write("Region Totals by Year out of Tolerance")
           st.write(tol_region_total_x_year)



