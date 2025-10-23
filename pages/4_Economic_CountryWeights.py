import streamlit as st
from app.myCountryModelPackages.Economic_Research import *
from app.myCountryModelPackages.sqlTableRetrieve import *
st.title("Economic Research Analysis")
st.write( \
    "This page is for validation and visualization of the Economic Research and GDP tables  These tables are generated dynamically so this was used to compre the results \
    of the generated Economic Research table in comparison to the VBA generated table.")

st.write( "")
st.write("The Economic Research table can be modified and will not be written back to the data base.")
st.write( "")
st.write("Filters will be added so you can drill down to the specific Region, Industry, etc.")

base_year = st.session_state['base_year']

economic_data = CountryEconomicResearch(base_year)
economic_research_VBA = economic_data.get_VBA_Generated_EconomicTable()
economic_research_compare = economic_data.get_Economic_Comparison()
economic_research_country_weight = economic_data.get_EconomicTable()

economic_research_data = Economic_Research_Create(base_year)
country_industrial_gdp_fraction = economic_research_data.get_industrial_gdp_fraction()
country_industrial_automation_degree = economic_research_data.get_industrial_automation_degree()
gdp_x_country = economic_research_data.get_gdp_x_country()

if 'base_year' in st.session_state:
    st.write("GDP by Country from Reuters upload, The Rest of xxx are calculated. ")
    st.write(gdp_x_country)
    st.write("Dynamically generated Country Weight Table derived the Industry GDP, Automation Degree by Industry, and Industry Weights")
    st.write("               countryWeight = gdp * industrialGdpFraction * industryFraction *automationDegree")
    st.write(" ")
    economic_research_country_weight["IndustryFraction"] = economic_research_country_weight["IndustryFraction"].map("{:.2%}".format)
    st.write(economic_research_country_weight)

