import streamlit as st
from myCountryModelPackages.CountryModel_Generation import *
from myCountryModelPackages.MarketReportRetrieval import MarketReports
from myCountryModelPackages.ProductTechnologyGroup import *
from myCountryModelPackages.sqlTableRetrieve import *
from myCountryModelPackages.CM_SessionStates import global_session_states_initialize, global_session_states_key

key_prefix = "CM_Generate_All_"
_GS_key_prefix = global_session_states_key()


if f"{key_prefix}ProductDescriptionTable" not in st.session_state:
    token = st.session_state[f"{_GS_key_prefix}msal_access_token"]
    st.session_state[f"{key_prefix}ProductDescriptionTable"] = ProductDescriptionTable(token)

st.title("Country Model Generator for All Reports in a Base Year")
st.write("You are updating all the reports from the selected base year. Generate a Country Model from an previously published World Wide market Report.")

# Two connections are provided so that there is flexibility to source the Worldwide Market study from a different
# database than where it is finally published
#   market_report_db_cxcn
#       Used to pull the published worldwide market report data which also includes the source of CountryKnowns
market_report_db_cxcn = DatabaseConnections().get_MiraLite_Connection()
 #   publication_db_engine
 #       database to publish the generated models.  need the sqlAlchemey engine connection
 #       different type of connection required for pushing data in to the database with sqlAlchemy
 #       also it gives flexibility to put it in a different database than the source reports.
publication_db_engine = DatabaseConnections().get_MiraLite_engine()

st.session_state['db_cxcn_market_research'] = market_report_db_cxcn

market_reports = MarketReports(market_report_db_cxcn)
report_list = market_reports.get_report_list()
report_list = report_list.sort_values(by='Study', ascending=True)
base_year_list = sorted(market_reports.get_base_year_list(), reverse=True )

col1, col2, col3 = st.columns(3)

if 'button' not in st.session_state:
     st.session_state.button = False
def click_button():
     st.session_state.button = not st.session_state.button

with col1:
     selected_base_year = st.selectbox('', base_year_list)
     st.session_state['base_year'] = selected_base_year

with col3:
    st.button('Start Model Generation for Selected Base Year', on_click=click_button)


col1, col2,col3 = st.columns(3)

with col1:
    if st.session_state.button:  # st.button('Create Country Model'):
        if selected_base_year in st.session_state.economic_model_years:
            valid_economic_model = ""
            with st.spinner("Loading data..."):
                # Iterate through the list and call the method
                base_year_reports = report_list[report_list['BaseYear'] == selected_base_year]
                base_year_reports = base_year_reports.drop(['BaseYear'],axis=1)
                for report in base_year_reports['Study']:
                    pdt = st.session_state[f"{key_prefix}ProductDescriptionTable"]
                    technology_group_name, technology_group_id = \
                        pdt.get_market_study_technology_group(report)
                    country_model = \
                        Country_Model_Generation(market_report_db_cxcn, report, selected_base_year, technology_group_id)
                    valid_report_message = country_model.validate_world_wide_report()
                    if valid_report_message == "":
                        country_share_model = country_model.generate_market_shares()
                        country_forecast_model = country_model.generate_forecast()
                        publish_model = Country_Model_Publish(publication_db_engine, report, selected_base_year,
                        country_share_model, country_forecast_model)
                        publish_model.publish_market_shares()
                        publish_model.publish_market_forecast()
                        st.write(f"Report: {report}  TechGroup: {technology_group_name}" )
                    else:
                        st.warning(report + " Country Model not generated due to the following reasons: \n" + valid_report_message )
                        CountryModelRemove(
                            st.session_state.db_engine_publication,
                            report,
                            selected_base_year).delete_country_model()
        else:
            st.markdown(f"""
                <h3 style='font-size:14pt;'>Unable to Generate Country Model due to the following reasons:</h3>
                <ul style='font-size:12pt;'>
                    <li>Economic Model Data not available for {selected_base_year}</li>
                </ul>
                """, unsafe_allow_html=True)

