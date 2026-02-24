import streamlit as st
from myCountryModelPackages.CountryModel_Generation import *
from myCountryModelPackages.CountryModel_RAS import *
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

st.session_state.setdefault(f"{key_prefix}ipf_results", None)

st.title("Country Model Generator")
text_message = "This application is designed to generate a Country Model from a published World Wide market Report. \
        Worldwide market  and Country Known data is necessary to run a model.  \
        Country Known data should be loaded in tandem with your worldwide report so that data is consistent."
st.markdown("<h3 style='font-size:16pt;'>" + text_message + "</h3>", unsafe_allow_html=True)

base_year_col, market_report_col, technology_group_col = st.columns([1,2,4])

def _start_model_generation():
    st.session_state[f"{key_prefix}generate_model_flag"] = True  #not st.session_state.button[f"{key_prefix}generate_model_button"]

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
    if not country_known_info.empty:
        country_known_info = country_known_info[country_known_info['Country'].isin(valid_countries)]
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

    styled_df = country_known_info.style.apply(
        lambda x: ['background-color: #DCECD1' if i % 2 == 0 else 'background-color: #ffffff' for i in range(len(x))],
        axis=0
    )

    if not country_known_info.empty:
        st.dataframe(styled_df, use_container_width=True)
    else:
        st.warning(f"Country Known Table NOT Loaded for {st.session_state[f"{key_prefix}base_year_select_value"]} -- {st.session_state[f"{key_prefix}market_report_select_value"]}")

with st.sidebar:
    st.header("Generate Model")
    st.button('Start',
              on_click=_start_model_generation,
              key=f"{key_prefix}generate_model_button")

col1, col2,col3 = st.columns(3)

with (col1):
    if st.session_state[f"{key_prefix}generate_model_flag"]:
       st.session_state[f"{key_prefix}generate_model_flag"] = False
       base_year_value = st.session_state[f"{key_prefix}base_year_select_value"]
       country_model = \
           Country_Model_Generation(st.session_state.db_cxcn_market_research,
                                    st.session_state[f"{key_prefix}market_report_select_value"],
                                    base_year_value,
                                    st.session_state[f"{key_prefix}technology_id"]
                                    )
       valid_report_message = country_model.validate_world_wide_report()

       if base_year_value in st.session_state[f"{_GS_key_prefix }economic_model_years"]:
            valid_economic_model = ""
       else:
           valid_economic_model = f"Economic Model Data not Available for: {st.session_state[f"{key_prefix}base_year_select_value"]}"

       if  valid_report_message == "" and valid_economic_model == "":
           with st.spinner("Generating Country Model and Publishing to the database..."):
               country_share_model = country_model.generate_market_shares()
               st.session_state['share_country_model'] = country_share_model

               country_forecast_model = country_model.generate_forecast()
               st.session_state['forecast_country_model'] = country_forecast_model
               publish_model_raw = \
                   Country_Model_Publish(st.session_state.db_engine_publication,
                                         st.session_state[f"{key_prefix}market_report_select_value"],
                                         st.session_state[f"{key_prefix}base_year_select_value"],
                                         country_share_model, country_forecast_model)
               publish_model_raw.publish_market_shares()
               publish_model_raw.publish_market_forecast()
               IPF_Correction = Country_Model_Forecast_RAS_Balancing(
                   st.session_state.db_cxcn_market_research,
                   st.session_state[f"{key_prefix}market_report_select_value"],
                   base_year_value,
                   50
               )
               country_model_share_aligned, country_model_forecast_aligned, n_iterations_share, n_iteration_x_year_forecast = IPF_Correction.ipf_align_country_model()
               publish_model_aligned = \
                   Country_Model_Publish(st.session_state.db_engine_publication,
                                         st.session_state[f"{key_prefix}market_report_select_value"],
                                         st.session_state[f"{key_prefix}base_year_select_value"],
                                         country_model_share_aligned, country_model_forecast_aligned)
               tol_totals, tol_region, tol_industry = IPF_Correction.verification_market_size_by_company_region_and_industry_with_worldwide()
               tol_base_year_industry_x_region, tol_industry_total_x_year, tol_region_total_x_year = IPF_Correction.verification_market_forecast_by_region_and_industry_with_worldwide()
               st.session_state[f"{key_prefix}ipf_results"] = {
                   "base_year":st.session_state['base_year'],
                   "market_report":st.session_state['market_report'],
                   "country_model_size_aligned": country_model_share_aligned,
                   "n_iterations_size": n_iterations_share,
                   "country_model_forecast_aligned": country_model_forecast_aligned,
                   "n_iteration_x_year_forecast": n_iteration_x_year_forecast,
                   "tol_totals": tol_totals,
                   "tol_region": tol_region,
                   "tol_industry": tol_industry,
                   "tol_base_year_industry_x_region": tol_base_year_industry_x_region,
                   "tol_industry_total_x_year": tol_industry_total_x_year,
                   "tol_region_total_x_year": tol_region_total_x_year,
               }
       else:
           warning_col = st.columns(1)[0]
           with col2:
               CountryModelRemove(st.session_state.db_engine_publication,
                                 st.session_state[f"{key_prefix}market_report_select_value"],
                                 st.session_state[f"{key_prefix}base_year_select_value"]).delete_country_model()
               st.markdown(f"""
                   <h3 style='font-size:14pt;'>Unable to Generate Country Model due to the following reasons:</h3>
                   <ul style='font-size:12pt;'>
                       <li>Market Report: {valid_report_message}</li>
                       <li>Economic Model: {valid_economic_model}</li>
                   </ul>
                   """, unsafe_allow_html=True)
               st.stop()

       with col1:
           st.write("Market Shares")
           st.write(country_model_share_aligned)
           st.spinner("Publishing IPF Aligned Country Market Share Model to the Database")
           publish_model_aligned.publish_market_shares()
           results = st.session_state[f"{key_prefix}ipf_results"]
           st.write("Iterations", results["n_iterations_size"])
           st.write("Company Totals out of Tolerance")
           st.write(results["tol_totals"])
           st.write("Company Industry out of Tolerance")
           st.write(results["tol_region"])
           st.write("Company Region out of Tolerance")
           st.write(results["tol_industry"])
       with col2:
           st.write("Market Forecast")
           st.write(country_model_forecast_aligned)
           st.spinner("Publishing IPF Aligned Country Model Forecast to the Database")
           publish_model_aligned.publish_market_forecast()
           st.write("Iterations Required by Year", results["n_iteration_x_year_forecast"])
           st.write("Industry Totals by Region on Base Year out of Tolerance")
           st.write(results["tol_base_year_industry_x_region"])
           st.write("Industry Totals by Year out of Tolerance")
           st.write(results["tol_industry_total_x_year"])
           st.write("Region Totals by Year out of Tolerance")
           st.write(results["tol_region_total_x_year"])