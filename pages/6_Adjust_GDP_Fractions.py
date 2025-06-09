import streamlit as st
from myCountryModelPackages.Economic_Research import *

initialize_global_session_states()

st.set_page_config(layout="wide")
st.title("Economic Research -  GDP Fractions by Country ")
st.write( "Economic Research GDP Fractions table is a set of weightings for each country.  This will allow to manipulate the size of the Country GDP. \
          The equation below is how the overall Country weight is calculated.")
st.write( " countryWeight = (gdp * industrialGdpFraction) * (industryFraction *automationDegree)" )
st.write( "")
st.write("The GDP Fractions can be modified and is pushed back to the data base.")

key_prefix = "GDP_Fractions_"
@st.cache_data
def get_base_year_list():
    return EconomicResearchFactorsRanges().get_gdp_research_years()
base_year_list = list(get_base_year_list())

if f"{key_prefix}region" not in st.session_state:
    if st.session_state.base_year is None:
        default_year = max(base_year_list)
    else:
        default_year = st.session_state.base_year
    all_regions = list(EconomicResearchFactorsRanges().get_regions_from_regional_remainder(default_year))
    st.session_state[f"{key_prefix}region"] = min(all_regions)

if f"{key_prefix}region_select_value" not in st.session_state:
    st.session_state[f"{key_prefix}region_select_value"] = st.session_state[f"{key_prefix}region"]

# Initialize session state for previous selections with current values if not set
if f"{key_prefix}prev_base_year" not in st.session_state:
    st.session_state[f"{key_prefix}prev_base_year"] = st.session_state.base_year
if f"{key_prefix}base_year_select_value" not in st.session_state:
    st.session_state[f"{key_prefix}base_year_select_value"] = st.session_state.base_year

if f"{key_prefix}prev_region" not in st.session_state:
    st.session_state[f"{key_prefix}prev_region"] = st.session_state[f"{key_prefix}region"]

if 'automation_gdp' not in st.session_state:
    st.session_state.automation_gdp = None

if f"{key_prefix}selection_committed" not in st.session_state:
    st.session_state[f"{key_prefix}selection_committed"] = False

if f"{key_prefix}show_research" not in st.session_state:
    st.session_state[f"{key_prefix}show_research"] = False

if f"{key_prefix}discard_changes" not in st.session_state:
    st.session_state[f"{key_prefix}discard_changes"] = False

if f"{key_prefix}save_changes" not in st.session_state:
    st.session_state[f"{key_prefix}save_changes"] = False

def discard_changes_button():
    st.session_state[f"{key_prefix}discard_changes"] = not st.session_state[f"{key_prefix}discard_changes"]
    st.session_state[f"{key_prefix}selection_committed"] = False

def save_changes_button():
    st.session_state[f"{key_prefix}save_changes"] = not st.session_state[f"{key_prefix}save_changes"]
    year = st.session_state.base_year
    region = st.session_state[f"{key_prefix}region"]
    #country = st.session_state.country
    db_modified =  st.session_state.automation_gdp
    db_modified['IndustrialGDP_Fraction'] = db_modified['IndustrialGDP_Fraction'] / 100
    EconomicResearchFactorsPublish().publish_region_gdp_fraction(db_modified, year, region )
    st.session_state[f"{key_prefix}selection_committed"] = False

def display_economic_research():
    #st.session_state.show_research = False
    economic_table, save_button, discard_changes = st.columns([2, 1, 1])
    with (economic_table):
        with st.spinner("Loading data..."):
            economic_factors = Economic_Research_Factors(st.session_state[f"{key_prefix}base_year_select_value"])
            automation_gdp = economic_factors.get_industry_gdp_fraction_region(st.session_state[f"{key_prefix}region_select_value"])
            automation_gdp['IndustrialGDP_Fraction'] = automation_gdp['IndustrialGDP_Fraction'] * 100.0
            st.write("Automation GDP by Region")
            revised_automation_gdp = \
                st.data_editor(
                    automation_gdp,
                    hide_index=True,
                    column_order=("Country", "Industry", "IndustrialGDP_Fraction"),
                    column_config={
                        "Country": st.column_config.TextColumn("Country", disabled=True, width="medium"),
                        "Industry": st.column_config.TextColumn("Industry", disabled=True, width="medium"),
                        "IndustrialGDP_Fraction": st.column_config.NumberColumn(
                            "IndustrialGDP_Fraction", format="%.2f%%", min_value=0.0, max_value=100.0, step=0.1, width="small"
                        )
                    }
                )
            st.session_state.automation_gdp = revised_automation_gdp
    with save_button:
        st.button('Save Modifications', on_click=save_changes_button)
    with discard_changes:
        st.button('Discard', on_click=discard_changes_button)

base_year_selection, region_selection, retrieve_selection = st.columns(3)

commit_status = st.session_state[f"{key_prefix}selection_committed"]
if not commit_status:
    # base_year_list = list(get_base_year_list())
    if not base_year_list:
        st.error("No base years available.")
        st.stop()
    # Initialize session state for base year
    if 'base_year' not in st.session_state:
        st.session_state.base_year = base_year_list[0]
    # Get region list based on base year
    region_list = list(EconomicResearchFactorsRanges().get_gdp_research_regions(st.session_state.base_year))
    if not region_list:
        st.error("No regions available for the selected base year.")
        st.stop()
    if 'region' not in st.session_state:
        st.session_state[f"{key_prefix}region"] = region_list[0]

    # UI layout
    with base_year_selection:
        #st.session_state.base_year = st.selectbox('Base Year', base_year_list, index=base_year_list.index(st.session_state.base_year), key=f"{key_prefix}base_year_select_value")
        st.session_state.base_year = st.selectbox('Base Year', base_year_list,
                                                  #index=base_year_list.index(st.session_state.base_year),
                                                  key=f"{key_prefix}base_year_select_value")
    with region_selection:
        region_list = list(EconomicResearchFactorsRanges().get_gdp_research_regions(st.session_state.base_year))
        st.session_state[f"{key_prefix}region"] = st.selectbox(
            'Region',
            region_list,
            #index=region_list.index(st.session_state[f"{key_prefix}prev_region"]),
            key=f"{key_prefix}region_select_value"  # This is the key used to track the widget's state
        )
        my_region_selected = st.session_state[f"{key_prefix}region_select_value"]

# Only reset if research is not currently being shown
if not st.session_state[f"{key_prefix}show_research"]:
    selection_changed = (
        st.session_state.base_year != st.session_state[f"{key_prefix}prev_base_year"] or
        st.session_state[f"{key_prefix}region"] != st.session_state[f"{key_prefix}prev_region"]
    )

    if selection_changed:
        st.session_state[f"{key_prefix}selection_committed"] = False
        st.session_state[f"{key_prefix}show_research"] = False

# Show button only if selection changed and not yet committed
show_display_button = not st.session_state[f"{key_prefix}selection_committed"]

def commit_selection():
    st.session_state[f"{key_prefix}selection_committed"] = True
    st.session_state[f"{key_prefix}show_research"] = True
    st.session_state[f"{key_prefix}prev_base_year"] = st.session_state[f"{key_prefix}base_year_select_value"]
    st.session_state[f"{key_prefix}prev_region"] = st.session_state[f"{key_prefix}region_select_value"]

with retrieve_selection:
    if not st.session_state[f"{key_prefix}selection_committed"]:
        st.button('Retrieve GDP Factors', on_click=commit_selection)

# Handle commit after rerun
##if st.session_state.get("commit_triggered", False):
#    st.session_state.show_research = True
#    st.session_state.selection_committed = True
#    st.session_state.prev_base_year = st.session_state['base_year']
#    st.session_state.prev_region = st.session_state['region']
#    st.session_state.prev_country = st.session_state['country']
#    st.session_state.commit_triggered = False  # Reset the trigger

if st.session_state[f"{key_prefix}show_research"]:
    display_economic_research()
    st.session_state[f"{key_prefix}prev_base_year"] = st.session_state[f"{key_prefix}base_year_select_value"]
    st.session_state[f"{key_prefix}prev_region"] = st.session_state[f"{key_prefix}region_select_value"]
    st.session_state[f"{key_prefix}show_research"] = False

