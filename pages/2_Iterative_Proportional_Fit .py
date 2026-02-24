import time
import streamlit as st
from myCountryModelPackages.CountryModel_RAS import *
from myCountryModelPackages.CountryModel_Generation import *
from myCountryModelPackages.sqlTableRetrieve import *
from myCountryModelPackages.MarketReportRetrieval import *
from myCountryModelPackages.ProductTechnologyGroup import *
from myCountryModelPackages.CM_SessionStates import global_session_states_initialize, global_session_states_key


st.set_page_config(layout="wide")

key_prefix = "SS_IPF_"

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

if f"{key_prefix}technology_id" not in st.session_state:
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

if f"{key_prefix}align_model_done"not in st.session_state:
    st.session_state[f"{key_prefix}align_model_done"] = False

if f"{key_prefix}align_in_progress"not in st.session_state:
    st.session_state[f"{key_prefix}align_in_progress"]= False

if f"{key_prefix}publish_model_flag"not in st.session_state:
    st.session_state[f"{key_prefix}publish_model_flag"] = False

if f"{key_prefix}iter_size"not in st.session_state:
    st.session_state[f"{key_prefix}iter_size"] = 25

st.session_state.setdefault(f"{key_prefix}ipf_results", None)

st.title("Country Model Alignment Using - Iterative Proportional Fit Algorithm ")
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

def _start_model_RAS_alignment():
    st.session_state[f"{key_prefix}align_model_flag"] = True  #not st.session_state.button[f"{key_prefix}generate_model_button"]

def _publish_RAS_aligned_model():
    st.session_state[f"{key_prefix}publish_model_flag"] = True #not st.session_state.button[f"{key_prefix}generate_model_button"]

publish_col, base_year_col, market_report_col, Iterations, technology_group_col = st.columns([1,1,2,1,4])
with (publish_col):
    st.markdown("<h1 style='font-size:12pt;text-align: center;'>Publish Status</h1>", unsafe_allow_html=True)
    # st.info("Inactive")
    if st.session_state[f"{key_prefix}publish_model_flag"]:
        with st.spinner("Publishing Model..."):
          #  st.warning("publication in progress")
          results = st.session_state[f"{key_prefix}ipf_results"]
          time.sleep(5.0)
          size_ipf = results["country_model_size_aligned"]
          forecast_ipf = results["country_model_forecast_aligned"]
          pub_cxcn = st.session_state.db_engine_publication
          publish_model = \
                Country_Model_Publish(st.session_state.db_engine_publication,
                                      results["market_report"],
                                      results["base_year"],
                                      results["country_model_size_aligned"],
                                      results["country_model_forecast_aligned"])
          status_size, errors_size = publish_model.publish_market_shares()
          status_forecast, errors_forecast = publish_model.publish_market_forecast()
          if status_size and status_forecast:

              st.success("PUBLISHED")
          else:
              st.error(f"Publish Failed: {errors_size} and {errors_forecast}")

          st.session_state[f"{key_prefix}publish_model_flag"]=False
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

with Iterations:
    st.markdown(
        f"<div style='line-height:1.2; font-size:14px;'><br><br> <br></div>",
        unsafe_allow_html=True
    )
    options = [25, 50, 75, 100, 200, 500]
    default_value = 20  # not the index

    iterations_selected_value = st.selectbox(
        "Iterations:",
        options=options,
        # index=options.index(st.session_state[key_name]),
        key=f"{key_prefix}iter_size"
    )
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

with st.sidebar:
    st.header("IPF Align")
    st.button(
        'START IPF',
        on_click=_start_model_RAS_alignment,
        key=f"{key_prefix}generate_model_button",
        disabled=st.session_state[f"{key_prefix}align_in_progress"]
        )

    st.button(
        'Publish (Visible Table)',
        on_click=_publish_RAS_aligned_model,
        key=f"{key_prefix}publish_model_button",
        disabled = not st.session_state[f"{key_prefix}align_model_done"]
    )

col1, col2,col3 = st.columns(3)

with (col1):
    if st.session_state[f"{key_prefix}align_model_flag"]:
       st.session_state[f"{key_prefix}align_model_flag"] = False
       base_year_value = st.session_state[f"{key_prefix}base_year_select_value"]
       st.session_state[f"{key_prefix}iter_size"]


       valid_report_message = ""   # placeholder to; will check whether a country model exists for this year and report

       if  valid_report_message == "":
           st.session_state[f"{key_prefix}align_model_done"] = False
           st.session_state[f"{key_prefix}align_in_progress"]= True
           with st.spinner("IPF Alignment of Country Model ..."):
               IPF_Correction = Country_Model_Forecast_RAS_Balancing(
                   st.session_state.db_cxcn_market_research,
                   st.session_state[f"{key_prefix}market_report_select_value"],
                   base_year_value,
                   iterations_selected_value
               )

               country_model_size_aligned, n_iterations_size = IPF_Correction.align_market_size_by_company_region_and_industry_with_worldwide()
               st.session_state[f"{key_prefix}share_country_model"] = country_model_size_aligned

               industry_by_region_base_year= (
                    country_model_size_aligned[country_model_size_aligned['BaseYear'] == base_year_value]
                    .groupby(['BaseYear', 'Region', 'Industry'])['Size']
                    .sum()
                    .to_frame('Target_Value')
                    .reset_index()
                    .rename(columns={'BaseYear': 'Year'})
               )

               country_model_forecast_aligned, n_iteration_x_year_forecast = IPF_Correction.align_forecast_by_region_and_industry(industry_by_region_base_year)
               st.session_state[f"{key_prefix}forecast_country_model"] = country_model_forecast_aligned

               st.session_state[f"{key_prefix}align_model_flag"] = False
               st.session_state[f"{key_prefix}align_model_done"] = True
               st.session_state[f"{key_prefix}align_in_progress"]= False

               # compute verification tables BEFORE rerun; store all artifacts
               tol_totals, tol_region, tol_industry = IPF_Correction.verification_market_size_by_company_region_and_industry_with_worldwide()
               tol_base_year_industry_x_region, tol_industry_total_x_year, tol_region_total_x_year = IPF_Correction.verification_market_forecast_by_region_and_industry_with_worldwide()

               st.session_state[f"{key_prefix}ipf_results"] = {
                   "base_year":st.session_state['base_year'],
                   "market_report":st.session_state['market_report'],
                   "country_model_size_aligned": country_model_size_aligned,
                   "n_iterations_size": n_iterations_size,
                   "country_model_forecast_aligned": country_model_forecast_aligned,
                   "n_iteration_x_year_forecast": n_iteration_x_year_forecast,
                   "tol_totals": tol_totals,
                   "tol_region": tol_region,
                   "tol_industry": tol_industry,
                   "tol_base_year_industry_x_region": tol_base_year_industry_x_region,
                   "tol_industry_total_x_year": tol_industry_total_x_year,
                   "tol_region_total_x_year": tol_region_total_x_year,
               }

               st.rerun()
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


results = st.session_state[f"{key_prefix}ipf_results"]
if results is not None:
    col0, col1 = st.columns(2)

    with col0:
        st.markdown("**Market Shares - IPF Aligned**")
        st.write(results["country_model_size_aligned"])
        st.write("Iterations", results["n_iterations_size"])
        st.write("Company Totals out of Tolerance")
        st.write(results["tol_totals"])
        st.write("Company Industry out of Tolerance")
        st.write(results["tol_region"])
        st.write("Company Region out of Tolerance")
        st.write(results["tol_industry"])

    with col1:
        st.markdown("**Country Model Forecast - IPF Aligned**")
        st.write(results["country_model_forecast_aligned"])
        st.write("Iterations Required by Year", results["n_iteration_x_year_forecast"])
        st.write("Industry Totals by Region on Base Year out of Tolerance")
        st.write(results["tol_base_year_industry_x_region"])
        st.write("Industry Totals by Year out of Tolerance")
        st.write(results["tol_industry_total_x_year"])
        st.write("Region Totals by Year out of Tolerance")
        st.write(results["tol_region_total_x_year"])



