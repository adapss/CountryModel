
import streamlit as st
from myCountryModelPackages.CountryModelEvaluationTools import *
from myCountryModelPackages.MarketReportRetrieval import *
# from pages.Economic import db_cxcn
from myCountryModelPackages.CountryModel_Generation import *
from myCountryModelPackages.MarketReportRetrieval import MarketReports
from myCountryModelPackages.sqlTableRetrieve import *
from myCountryModelPackages.MarketReportRetrieval import *
from myCountryModelPackages.CountryModelEvaluationTools import *

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
base_year_list = market_reports.get_base_year_list().sort_values(ascending=False)
# Add buttons for user actions
col1, col2, col3 = st.columns(3)

if 'button' not in st.session_state:
     st.session_state.button = False
def click_button():
     st.session_state.button = not st.session_state.button

 # Display the Report List

with col1:
     # st.write('Market Report Selection:')
     #selected_base_year = st.selectbox(f'Select Base Year', base_year_list)
     selected_base_year = st.selectbox('', base_year_list)
     st.session_state['base_year'] = selected_base_year
with col3:
    # st.button('Create Country Model') #st.caption("Press the button to generate Country Model tables")
    st.button('Start Model Generation for Selected Base Year', on_click=click_button)

 # Add buttons for user actions
col1, col2,col3 = st.columns(3)
 #with col1:
 #   st.caption("Press the button to generate Country Model tables")

with col1:
    if st.session_state.button:  # st.button('Create Country Model'):
        with st.spinner("Loading data..."):
            # Iterate through the list and call the method
            base_year_reports = report_list[report_list['BaseYear'] == selected_base_year]
            base_year_reports = base_year_reports.drop(['BaseYear'],axis=1)
            for report in base_year_reports['Study']:
                country_model = Country_Model_Generation(market_report_db_cxcn, report, selected_base_year)
                country_share_model = country_model.generate_market_shares()
                country_forecast_model = country_model.generate_forecast()
                publish_model = Country_Model_Publish(publication_db_engine, report, selected_base_year,
                country_share_model, country_forecast_model)
                publish_model.publish_market_shares()
                publish_model.publish_market_forecast()
                st.write(report)


