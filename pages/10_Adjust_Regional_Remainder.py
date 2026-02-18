import time
import streamlit as st
from myCountryModelPackages.Economic_Research import *
from myCountryModelPackages.CM_SessionStates import global_session_states_initialize
import numpy as np

key_prefix = "Region_Rem_"
global_session_states_initialize()

st.set_page_config(layout="wide")
st.title("Economic Research -  GDP Regional Remainders ")
st.write( "")
st.write("The GDP Regional Remainders can be modified and is pushed back to the data base.")

@st.cache_data
def get_base_year_list():
    return EconomicResearchFactorsRanges().getlist_cm_gdp_regional_remainder_research_years()
base_year_list = list(get_base_year_list())

if f"{key_prefix}tg_id_value" not in st.session_state:
    st.session_state[f"{key_prefix}tg_id_value"] = 0

if 'region' not in st.session_state:
    if st.session_state.base_year is None:
        default_year = max(base_year_list)
    else:
        default_year = st.session_state.base_year
    all_regions = list(EconomicResearchFactorsRanges().getlist_cm_regions_from_regional_remainder(default_year))
    st.session_state.region = min(all_regions)

if 'automation_gdp' not in st.session_state:
    st.session_state.automation_gdp = None

if 'selection_committed_reg_rem' not in st.session_state:
    st.session_state.selection_committed_reg_rem = False

if 'show_research' not in st.session_state:
    st.session_state.show_research = False

if 'discard_changes' not in st.session_state:
    st.session_state.discard_changes = False
    st.session_state.show_research = True

if 'save_changes' not in st.session_state:
    st.session_state.save_changes = False

def discard_changes_button():
    st.session_state.discard_changes = not st.session_state.discard_changes
    st.session_state.selection_committed_reg_rem = False
    st.session_state.save_changes = False
    st.session_state.show_research = True


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
    remainder_table, = st.columns(1) # , save_button, discard_changes = st.columns([2, 1, 1])
    with remainder_table:
        with st.spinner("Loading data..."):
            economic_factors = \
                Economic_Research_Factors(
                    st.session_state.base_year_select_value,
                    st.session_state[f"{key_prefix}tg_id_value"]
                )
            discard_triggered = st.session_state.get("discard_changes", False)
            if discard_triggered:
                st.session_state.discard_changes = False
            editor_key = st.session_state.get("editor_key", "data_editor_key")
            gdp_remainders = economic_factors.get_regional_remainder_region(st.session_state.region_select_value)
            gdp_remainders['RemainderSize'] = gdp_remainders['RemainderSize'] * 100.0
            gdp_remainders['RemainderSize'] = (
                    np.ceil(gdp_remainders['RemainderSize'] * 100) / 100
            )
            st.write("GDP Regional Remainders as a Percentage of the Total Market")
            with st.form("edit_form"):
                revised_gdp_remainders = st.data_editor(
                    gdp_remainders,
                    key=editor_key,
                    hide_index=True,
                    column_order=("Country", "RemainderSize"),
                    column_config={
                        "Country": st.column_config.TextColumn("Country", disabled=True, width="medium"),
                        "RemainderSize": st.column_config.NumberColumn(
                            "RemainderSize(%)", format="%.2f%%", min_value=0.0, max_value=100.0, step=0.01, width="small"
                        )
                    }
                )
                form_col1, form_col2 = st.columns(2)
                with form_col1:
                    apply_changes = st.form_submit_button("Apply Changes")

                with form_col2:
                    discard_changes = st.form_submit_button("Discard Changes")

            # ✅ Handle actions AFTER the form block
    if apply_changes:
        st.session_state.automation_gdp = revised_gdp_remainders
        save_changes_button()  # Your function

    if discard_changes:
        st.session_state.editor_key = f"data_editor_key_{int(time.time())}"
        discard_changes_button()
        st.rerun()

# Initialize session state for previous selections with current values if not set
if 'prev_base_year' not in st.session_state:
    st.session_state.prev_base_year = st.session_state.get('base_year')
if 'prev_region' not in st.session_state:
    st.session_state.prev_region = st.session_state.get('region')

base_year_selection, region_selection = st.columns([1,3])   #, retrieve_selection

commit_status = st.session_state.selection_committed_reg_rem
if not commit_status:
    if not base_year_list:
        st.error("No base years available.")
        st.stop()
    if 'base_year' not in st.session_state:
        st.session_state.base_year = base_year_list[0]
    # Get region list based on base year
    if 'base_year_select_value' not in st.session_state:
        region_list = list(EconomicResearchFactorsRanges().getlist_cm_regions_from_regional_remainder(st.session_state.base_year))
    else:
        region_list = list(EconomicResearchFactorsRanges().getlist_cm_regions_from_regional_remainder(st.session_state.base_year_select_value))
    if not region_list:
        st.error("No regions available for the selected base year.")
        st.stop()
    if 'region' not in st.session_state:
        st.session_state.region = region_list[0]

    with base_year_selection:
        st.session_state.bae_year = st.selectbox('Base Year', base_year_list, index=base_year_list.index(st.session_state.base_year), key='base_year_select_value')
    with region_selection:
        region_list = list(EconomicResearchFactorsRanges().getlist_cm_regions_from_regional_remainder(st.session_state.base_year_select_value))
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
    st.session_state.show_research = True
    st.session_state.prev_base_year = st.session_state.base_year
    st.session_state.prev_region = st.session_state.region

temp = st.session_state.show_research
display_economic_research()



