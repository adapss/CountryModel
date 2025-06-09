import streamlit as st
from myCountryModelPackages.Economic_Research import *

initialize_global_session_states()

st.set_page_config(layout="wide")
st.title("Economic Research -  GDP Regional Remainders ")
st.write( "")
st.write("The GDP Regional Remainders can be modified and is pushed back to the data base.")

@st.cache_data
def get_base_year_list():
    return EconomicResearchFactorsRanges().get_gdp_regional_remainder_research_years()
base_year_list = list(get_base_year_list())

if 'region' not in st.session_state:
    if st.session_state.base_year is None:
        default_year = max(base_year_list)
    else:
        default_year = st.session_state.base_year
    all_regions = list(EconomicResearchFactorsRanges().get_regions_from_regional_remainder(default_year))
    st.session_state.region = min(all_regions)

if 'automation_gdp' not in st.session_state:
    st.session_state.automation_gdp = None

if 'selection_committed_reg_rem' not in st.session_state:
    st.session_state.selection_committed_reg_rem = False

if 'show_research' not in st.session_state:
    st.session_state.show_research = False

if 'discard_changes' not in st.session_state:
    st.session_state.discard_changes = False

if 'save_changes' not in st.session_state:
    st.session_state.save_changes = False

def discard_changes_button():
    st.session_state.discard_changes = not st.session_state.discard_changes
    st.session_state.selection_committed_reg_rem = False

def save_changes_button():
    st.session_state.save_changes = not st.session_state.save_changes
    my_year = st.session_state.base_year
    my_region = st.session_state.region
    #country = st.session_state.country
    db_modified =  st.session_state.automation_gdp
    db_modified['RemainderSize'] = db_modified['RemainderSize'] / 100
    EconomicResearchFactorsPublish().publish_regional_gdp_remainders(db_modified, my_year, my_region )
    st.session_state.selection_committed_reg_rem = False

def display_economic_research():
    #st.session_state.show_research = False
    remainder_table, save_button, discard_changes = st.columns([2, 1, 1])
    with (remainder_table):
        with st.spinner("Loading data..."):
            economic_factors = Economic_Research_Factors(st.session_state.base_year_select_value)
            gdp_remainders = economic_factors.get_regional_remainder_region(st.session_state.region_select_value)
            gdp_remainders['RemainderSize'] = gdp_remainders['RemainderSize'] * 100.0
            st.write("GDP Regional Remainders as a Percentage of the Total Market")
            revised_gdp_remainders = \
                st.data_editor(
                    gdp_remainders,
                    hide_index=True,
                    column_order=("Country", "RemainderSize"),
                    column_config={
                        "Country": st.column_config.TextColumn("Country", disabled=True, width="medium"),
                        "RemainderSize": st.column_config.NumberColumn(
                            "RemainderSize", format="%.2f%%", min_value=0.0, max_value=100.0, step=0.1, width="small"
                        )
                    }
                )
            st.session_state.automation_gdp = revised_gdp_remainders
    with save_button:
        st.button('Save', on_click=save_changes_button)
    with discard_changes:
        st.button('Discard', on_click=discard_changes_button)

# Initialize session state for previous selections with current values if not set
if 'prev_base_year' not in st.session_state:
    st.session_state.prev_base_year = st.session_state.get('base_year')
if 'prev_region' not in st.session_state:
    st.session_state.prev_region = st.session_state.get('region')

base_year_selection, region_selection, retrieve_selection = st.columns(3)

commit_status = st.session_state.selection_committed_reg_rem
if not commit_status:
    # base_year_list = list(get_base_year_list())
    if not base_year_list:
        st.error("No base years available.")
        st.stop()
    # Initialize session state for base year
    if 'base_year' not in st.session_state:
        st.session_state.base_year = base_year_list[0]
    # Get region list based on base year
    if 'base_year_select_value' not in st.session_state:
        region_list = list(EconomicResearchFactorsRanges().get_regions_from_regional_remainder(st.session_state.base_year))
    else:
        region_list = list(EconomicResearchFactorsRanges().get_regions_from_regional_remainder(st.session_state.base_year_select_value))
    if not region_list:
        st.error("No regions available for the selected base year.")
        st.stop()
    if 'region' not in st.session_state:
        st.session_state.region = region_list[0]

    # UI layout
    with base_year_selection:
        st.session_state.bae_year = st.selectbox('Base Year', base_year_list, index=base_year_list.index(st.session_state.base_year), key='base_year_select_value')
    with region_selection:
        region_list = list(EconomicResearchFactorsRanges().get_regions_from_regional_remainder(st.session_state.base_year_select_value))
        region_selected = st.session_state.region
        st.session_state.region = st.selectbox('Region', region_list, key = 'region_select_value')

# Only reset if research is not currently being shown
if not st.session_state.show_research:
    selection_changed = (
        st.session_state.base_year != st.session_state.prev_base_year or
        st.session_state.region != st.session_state.prev_region
    )

    if selection_changed:
        st.session_state.selection_committed_reg_rem = False
        st.session_state.show_research = False

# Show button only if selection changed and not yet committed
show_display_button = not st.session_state.selection_committed_reg_rem

def commit_selection():
    st.session_state.selection_committed_reg_rem = True
    st.session_state.show_research = True
    st.session_state.prev_base_year = st.session_state.base_year
    st.session_state.prev_region = st.session_state.region

with retrieve_selection:
    if not st.session_state.selection_committed_reg_rem:
        st.button('Retrieve GDP Factors', on_click=commit_selection)

if st.session_state.show_research:
    #year = st.session_state.base_year
    #region = st.session_state.region
    display_economic_research()
    # ✅ Now that the table is shown, update previous selections
    st.session_state.prev_base_year = st.session_state.base_year
    st.session_state.prev_region = st.session_state.region
    st.session_state.show_research = False


