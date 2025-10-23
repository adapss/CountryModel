import time
from typing import Optional

import streamlit as st
from streamlit import session_state
from app.myCountryModelPackages.Economic_Research import *
from app.myCountryModelPackages.CM_SessionStates import initialize_global_session_states
initialize_global_session_states()
import numpy  as np

key_prefix = "Industry_Weights_"


st.set_page_config(layout="wide")
st.title("Economic Research - Industry Weights by Country")
text_caption = "This page is intended to create a sandbox where you can modify the Economic Research table.  \
    You can modify the weightings for each country industry.  Then run the country model, analyze results \
    before publishing to the database.  Once you are satisfied, then the Economic Research will be published and the Country Model \
    for that report will be published as well."
st.markdown("<h3 style='font-size:16pt;'>" + text_caption + "</h3>", unsafe_allow_html=True)
st.write( "")
text_caption = "The Economic Research table can be modified and is pushed back to the data base."
st.markdown("<h3 style='font-size:16pt;'>" + text_caption + "</h3>", unsafe_allow_html=True)

@st.cache_data
def __get_base_year_list():
    return EconomicResearchFactorsRanges().get_industry_weights_research_years()
base_year_list = list(__get_base_year_list())

@st.cache_data
def __get_region_list(year: Optional[int] = None):
    if year is None:
        year = st.session_state.base_year
    try:
        if year is None:
            if 'base_year' not in st.session_state:
                st.error("Base year is not set in session state.")
                return []
            year = st.session_state.base_year

        regions = EconomicResearchFactorsRanges().get_industry_weights_research_regions(year)

        if not regions:
            st.warning(f"No regions found for year {year}.")
            return []

        return list(regions)

    except AttributeError as e:
        st.error(f"Session state or EconomicResearchFactorsRanges is misconfigured: {e}")
        return []
    except Exception as e:
        st.error(f"Unexpected error while fetching regions: {e}")
        return []

@st.cache_data
def __get_country_list (year, region):
    return list(EconomicResearchFactorsRanges().get_countries_from_industry_weights_research(st.session_state[f"{key_prefix}base_year_select_value"],st.session_state[f"{key_prefix}region_select_value"]))

with (st.spinner("Initializing page selections")):
    if f"{key_prefix}duplicate_value_flag" not in session_state:
        st.session_state[f"{key_prefix}duplicate_value_flag"] = False
    if f"{key_prefix}base_year_select_value" not in session_state:
        st.session_state[f"{key_prefix}base_year_select_value"] = st.session_state.base_year
    if f"{key_prefix}region_select_value" not in session_state:
        all_regions = __get_region_list(st.session_state.base_year)
        #list(EconomicResearchFactorsRanges().get_industry_weights_research_regions(st.session_state.base_year))
        st.session_state[f"{key_prefix}region_list"] = all_regions
        st.session_state[f"{key_prefix}region_select_value" ] = max(all_regions)
    if f"{key_prefix}country_select_value" not in session_state:
        country_default =  max(__get_country_list(st.session_state[f"{key_prefix}base_year_select_value"],st.session_state[f"{key_prefix}region_select_value"]))
        #max(list(EconomicResearchFactorsRanges().get_countries_from_industry_weights_research(st.session_state[f"{key_prefix}base_year_select_value"],st.session_state[f"{key_prefix}region_select_value"])))
        st.session_state[f"{key_prefix}country_select_value"] = country_default

    # Initialize session state for previous selections with current values if not set
    if f"{key_prefix}prev_base_year" not in st.session_state:
        st.session_state[f"{key_prefix}prev_base_year"] = st.session_state[f"{key_prefix}base_year_select_value"]
    if f"{key_prefix}prev_region" not in st.session_state:
        st.session_state[f"{key_prefix}prev_region"] = st.session_state[f"{key_prefix}region_select_value"]
    if f"{key_prefix}prev_country" not in st.session_state:
        st.session_state[f"{key_prefix}prev_country"] = st.session_state[f"{key_prefix}country_select_value"]
    if 'industry_fraction_table' not in session_state:
        st.session_state.industry_fraction_table = Economic_Research_Factors(st.session_state[f"{key_prefix}base_year_select_value"]).get_industry_fractions_country(st.session_state[f"{key_prefix}country_select_value"])

    if 'selection_committed_weights' not in st.session_state:
        st.session_state.selection_committed_weights = False

    if 'show_research_weights' not in st.session_state:
        st.session_state.show_research_weights = False

    if 'discard_changes' not in st.session_state:
        st.session_state.discard_changes = False
        st.session_state.show_research_weights = False

    if 'save_changes' not in st.session_state:
        st.session_state.save_changes = False

def discard_changes_button():
    st.session_state.discard_changes = not st.session_state.discard_changes
    st.session_state.selection_committed_weights = False
    st.session_state.show_research_weights = False

def save_changes_button():
    st.session_state.save_changes = not st.session_state.save_changes
    db_modified =  st.session_state.industry_fraction_table
    inital_sum = db_modified['IndustryFraction'].sum()
    db_modified['IndustryFraction'] = db_modified['IndustryFraction']/db_modified['IndustryFraction'].sum()
    verify_total  = db_modified['IndustryFraction'].sum()
    EconomicResearchFactorsPublish().publish_industry_weights(db_modified, st.session_state[f"{key_prefix}base_year_select_value"], st.session_state[f"{key_prefix}region_select_value"], st.session_state[f"{key_prefix}country_select_value"] )
    st.session_state.selection_committed_weights = False
    st.session_state.show_research_weights = False

def _editor_table_change():
    st.session_state.show_research_weights = True

def display_economic_research():
    #st.session_state.show_research_weights = False
    economic_table, = st.columns(1) #save_button, discard_changes, clean_duplicates = st.columns([3, 1, 1,1])
    with economic_table:
        with st.spinner("Loading data..."):
            economic_factors = Economic_Research_Factors(st.session_state[f"{key_prefix}base_year_select_value"])
            #automation_degree = economic_factors.get_automation_degree_country(st.session_state[f"{key_prefix}country_select_value"])
            discard_triggered = st.session_state.get("discard_changes", False)
            if discard_triggered:
                st.session_state.discard_changes = False
            editor_key = st.session_state.get("editor_key", "data_editor_key")

            industry_weights = economic_factors.get_industry_fractions_country(
                    st.session_state[f"{key_prefix}country_select_value"]
                )

            if industry_weights.duplicated(subset=['BaseYear', 'Region','Country', 'Industry']).any():
                st.write("Duplicates found in table....")
                st.session_state[f"{key_prefix}duplicate_value_flag"] = True
            else:
                st.write("Duplicates NOT Found")
                st.session_state[f"{key_prefix}duplicate_value_flag"] = False
            industry_weights['IndustryFraction'] = industry_weights['IndustryFraction'] * 100.0
            industry_weights['IndustryFraction'] = (
                    np.ceil(industry_weights['IndustryFraction'] * 100) / 100
            )
            st.write("Industry Weights by Country")
            with st.form("edit_form"):
                    revised_industry_fractions = \
                        st.data_editor(
                            industry_weights.sort_values(by="Industry", ascending=True), #automation_degree.head(len(automation_degree)),
                            hide_index=True,
                            num_rows="fixed",
                            key= editor_key,
                            column_order=("BaseYear","Country", "Industry", "AutomationDegree", "IndustryFraction"),
                            column_config={
                                "BaseYear": st.column_config.NumberColumn(
                                    "Year", format="%.0f", min_value=0, max_value=10, width="small"
                                ),
                                "Country": st.column_config.TextColumn("Country", disabled=True, width="medium"),
                                "Industry": st.column_config.TextColumn("Industry", disabled=True, width="medium"),
                                "IndustryFraction": st.column_config.NumberColumn(
                                    "Industry Fraction(%)", format="%.2f", step=0.01, min_value=0.0, max_value=100.0, width="small"
                                )
                            }
                        )
                    form_col1, form_col2 = st.columns(2)
                    with form_col1:
                        apply_changes = st.form_submit_button("Apply Changes")

                    with form_col2:
                        discard_changes = st.form_submit_button("Discard Changes")

#            st.session_state.automation_degree_table = revised_automation_degree
    if apply_changes:
        st.session_state.industry_fraction_table = revised_industry_fractions
        save_changes_button()  # Your function
    if discard_changes:
        st.session_state.editor_key = f"data_editor_key_{int(time.time())}"
        discard_changes_button()
        st.rerun()

base_year_selection_col, region_selection_col, country_selection_col = st.columns(3)

status =st.session_state.show_research_weights
commit_status = st.session_state.selection_committed_weights
if not commit_status:
    # base_year_list = list(get_base_year_list())
    if not base_year_list:
        st.error("No base years available.")
        st.stop()

    if not st.session_state[f"{key_prefix}region_list"]:
        st.error("No regions available for the selected base year.")
        st.stop()
    # Get country list based on base year and region
    country_list =  __get_country_list(st.session_state[f"{key_prefix}base_year_select_value"],st.session_state[f"{key_prefix}region_select_value"])
    #list(EconomicResearchFactorsRanges().get_countries_from_industry_weights_research(st.session_state[f"{key_prefix}base_year_select_value"], st.session_state[f"{key_prefix}region_select_value"]))
    if not country_list:
        st.error("No countries available for the selected region.")
        st.stop()
    if f"{key_prefix}country" not in st.session_state:
        st.session_state[f"{key_prefix}country"] = country_list[0]


    with base_year_selection_col:
        _test = st.session_state[f"{key_prefix}base_year_select_value"]
        st.selectbox('Base Year', base_year_list,
                     # index=base_year_list.index(st.session_state.base_year),
                     key=f"{key_prefix}base_year_select_value")
    with region_selection_col:
       # if st.session_state[f"{key_prefix}region_select_value"] !=  st.session_state[f"{key_prefix}prev_region"]:
       #     st.session_state[f"{key_prefix}region_list"] = list(EconomicResearchFactorsRanges().get_industry_weights_research_regions(st.session_state[f"{key_prefix}base_year_select_value"]))
        matching_index =  st.session_state[f"{key_prefix}region_list"].index(st.session_state[f"{key_prefix}region_select_value"])
        st.selectbox('Region', st.session_state[f"{key_prefix}region_list"], index = matching_index, key=f"{key_prefix}region_select_value")
    with (country_selection_col):
        country_list = __get_country_list(
            st.session_state[f"{key_prefix}base_year_select_value"],
            st.session_state[f"{key_prefix}region_select_value"]
        )
        #list(EconomicResearchFactorsRanges().get_countries_from_industry_weights_research(st.session_state[f"{key_prefix}base_year_select_value"],st.session_state[f"{key_prefix}region_select_value"]))
        if  st.session_state[f"{key_prefix}country_select_value"] not in country_list:
            st.session_state[f"{key_prefix}country_select_value"] = country_list[0]
            debug_country = st.session_state[f"{key_prefix}country_select_value"]
        else:
            debug_country = st.session_state[f"{key_prefix}country_select_value"]
        country_index = country_list.index(st.session_state[f"{key_prefix}country_select_value"])
        st.selectbox('Country', country_list, key=f"{key_prefix}country_select_value")
        country_value = st.session_state[f"{key_prefix}country_select_value"]
        country_value1 = country_value

if not st.session_state.show_research_weights:
    selection_changed = (
        st.session_state.base_year != st.session_state[f"{key_prefix}prev_base_year"] or
        st.session_state[f"{key_prefix}region_select_value"] != st.session_state[f"{key_prefix}prev_region"] or
        st.session_state[f"{key_prefix}country_select_value"] != st.session_state[f"{key_prefix}prev_country"]
    )
    if selection_changed:
        st.session_state.selection_committed_weights = False
        st.session_state.show_research_weights = False

# Show button only if selection changed and not yet committed
show_display_button = not st.session_state.selection_committed_weights

def commit_selection():
    st.session_state.selection_committed_weights = True
    st.session_state.show_research_weights = True
    st.session_state[f"{key_prefix}prev_base_year"] = st.session_state[f"{key_prefix}base_year_select_value"]
    st.session_state[f"{key_prefix}prev_region"] = st.session_state[f"{key_prefix}region_select_value"]
    st.session_state[f"{key_prefix}prev_country"] = st.session_state[f"{key_prefix}country_select_value"]
    st.session_state[f"{key_prefix}display_research_button"] = False


display_economic_research()

