import streamlit as st
from myCountryModelPackages.Economic_Research import *
from myCountryModelPackages.CM_SessionStates import global_session_states_initialize, global_session_states_key
from myCountryModelPackages.ProductTechnologyGroup import *

key_prefix = "CM_Economic_Research_"
_GS_key_prefix = global_session_states_key()

if f"{key_prefix}TechnologyGroups" not in st.session_state:
    token = st.session_state[f"{_GS_key_prefix}msal_access_token"]
    economic_research = EconomicResearchFactorsRanges()
    st.session_state[f"{key_prefix}TechnologyGroups"] = economic_research.getlist_cm_gdp_research_technology_groups(st.session_state['base_year'] )

if f"{key_prefix}retrieve_economic_data_flag" not in st.session_state:
    st.session_state[f"{key_prefix}retrieve_economic_data_flag"] = False

if f"{key_prefix}TechnologyGroup_Select" not in st.session_state:
    st.session_state[f"{key_prefix}TechnologyGroup_Select"] = 0

if f"{key_prefix}economic_year_list" not in st.session_state:
    economic_research = EconomicResearchFactorsRanges()
    base_year_list_economic_research = economic_research.get_economic_research_years()
    st.session_state[f"{key_prefix}economic_year_list"] = base_year_list_economic_research

st.title("Economic Research Analysis - Country Weights and GDP ")
st.write( \
    "This page is for validation and visualization of the Economic Research and GDP tables .")

def click_button():
    st.session_state[f"{key_prefix}retrieve_economic_data_flag"] = True

generate_economic_data_button, base_year_col, technology_group_col = st.columns([1,1,3])

with base_year_col:
    st.markdown("<h1 style='font-size:12pt;text-align: center;'>Base Year</h1>", unsafe_allow_html=True)
    st.selectbox('Select Base Year', st.session_state[f"{key_prefix}economic_year_list"],key = f"{key_prefix}base_year_select_value")
    st.session_state['base_year'] =  st.session_state[f"{key_prefix}base_year_select_value"]
    economic_research = EconomicResearchFactorsRanges()
    st.session_state[f"{key_prefix}TechnologyGroups"] =  economic_research.getlist_cm_gdp_research_technology_groups(st.session_state[f"{key_prefix}base_year_select_value"])

with technology_group_col:
    st.markdown("<h1 style='font-size:12pt;text-align: center;'>Technology Group</h1>", unsafe_allow_html=True)
    tgf = st.session_state[f"{key_prefix}TechnologyGroups"]
    st.session_state[f"{key_prefix}TechnologyGroup_Select"] = \
        st.selectbox('Select Technology Group', tgf, key = f"{key_prefix}technology_group_select_value")

with generate_economic_data_button:
    st.button('Retrieve Economic Data',
              on_click=click_button,
              key=f"{key_prefix}retrieve_economic_data_button")

economic_data_col, = st.columns(1)

with (economic_data_col):
    if st.session_state[f"{key_prefix}retrieve_economic_data_flag"]:
        st.session_state[f"{key_prefix}retrieve_economic_data_flag"] = False
        if 'base_year' in st.session_state:
            base_year = st.session_state['base_year']
            tgf = st.session_state[f"{key_prefix}TechnologyGroups"]
            tg_name = st.session_state[f"{key_prefix}TechnologyGroup_Select"]
            tg_name1 = st.session_state[f"{key_prefix}technology_group_select_value"]
            pdt = ProductDescriptionTable(st.session_state[f"{_GS_key_prefix}msal_access_token"])
            tg_id = pdt.lookup_technology_group_id(tg_name1 )

            economic_data = CountryEconomicResearch(base_year, tg_id)
            economic_research_VBA = economic_data.get_VBA_Generated_EconomicTable()
            economic_research_compare = economic_data.get_Economic_Comparison()
            economic_research_country_weight = economic_data.get_EconomicTable()

            economic_research_data = EconomicResearchCreate(base_year, tg_id)
            country_industrial_gdp_fraction = economic_research_data.get_industrial_gdp_fraction()
            country_industrial_automation_degree = economic_research_data.get_industrial_automation_degree()
            gdp_x_country = economic_research_data.get_gdp_x_country()
            st.write("GDP by Country from Reuters upload, The Rest of xxx are calculated. ")
            st.write(gdp_x_country)
            st.write("Dynamically generated Country Weight Table derived the Industry GDP, Automation Degree by Industry, and Industry Weights")
            st.write("               countryWeight = gdp * industrialGdpFraction * industryFraction *automationDegree")
            st.write(" ")
            economic_research_country_weight["IndustryFraction"] = economic_research_country_weight["IndustryFraction"].map("{:.2%}".format)
            st.write(economic_research_country_weight)

