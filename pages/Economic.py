import streamlit as st
from myCountryModelPackages.Economic_Research import *


st.title("Economic Research Analysis")
st.write( \
    "This page is intended to create a sandbox where you can modify the Economic Research table.  \
    You can modify the weightings for each country industry.  Then run the country model, analyze results \
    before publishing to the database.  Once you are satisfied, then the Economic Research will be published and the Country Model \
    for that report will be published as well.  Currently, this is the raw Economic Research table that is derived from the Economic Spreadsheet \
    To make this easier to manipulate, we will create data entry similar or exactly the same as the spreadsheet and perform all the conversions \
    behind the scenes.")

st.write( "")
st.write("The Economic Research table can be modified and will not be written back to the data base.")
st.write( "")
st.write("Filters will be added so you can drill down to the specific Region, Industry, etc.")

db_cxcn = st.session_state['db_cxcn']
base_year = st.session_state['base_year']

economic_research = CountryEconomicResearch(db_cxcn, base_year).get_EconomicTable()
if 'base_year' in st.session_state:
    st.data_editor(economic_research,disabled=("Country", "Region","BaseYear","Industry"))
    #st.dataframe(economic_research)
