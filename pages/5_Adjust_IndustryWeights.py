import streamlit as st
from streamlit import session_state
from myCountryModelPackages.Economic_Research import *

initialize_global_session_states()

key_prefix = "Industry_Weights_"


st.set_page_config(layout="wide")
st.title("Economic Research - Industry Weights and Automation Degree by Country")
st.write( "This page is intended to create a sandbox where you can modify the Economic Research table.  \
    You can modify the weightings for each country industry.  Then run the country model, analyze results \
    before publishing to the database.  Once you are satisfied, then the Economic Research will be published and the Country Model \
    for that report will be published as well.")
st.write( "")
st.write("The Economic Research table can be modified and is pushed back to the data base.")

@st.cache_data
def __get_base_year_list():
    return EconomicResearchFactorsRanges().get_industry_weights_research_years()
base_year_list = list(__get_base_year_list())

@st.cache_data
def __get_region_list(year):
    return list(EconomicResearchFactorsRanges().get_industry_weights_research_regions(st.session_state.base_year))

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
    if 'automation_degree_table' not in session_state:
        st.session_state.automation_degree_table = Economic_Research_Factors(st.session_state[f"{key_prefix}base_year_select_value"]).get_automation_degree_country(st.session_state[f"{key_prefix}country_select_value"])

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
def clean_duplicates_button():
    automation_degree= Economic_Research_Factors(st.session_state[f"{key_prefix}base_year_select_value"]).get_automation_degree_country(st.session_state[f"{key_prefix}country_select_value"])
    automation_degree = automation_degree.drop_duplicates(subset=['BaseYear', 'Region','Country', 'Industry'])
    EconomicResearchFactorsPublish().publish_industry_weights(automation_degree, st.session_state[f"{key_prefix}base_year_select_value"], st.session_state[f"{key_prefix}region_select_value"], st.session_state[f"{key_prefix}country_select_value"] )
    st.session_state[f"{key_prefix}duplicate_value_flag"] = False

def save_changes_button():
    st.session_state.save_changes = not st.session_state.save_changes
    db_modified =  st.session_state.automation_degree_table
    db_modified['IndustryFraction'] = db_modified['IndustryFraction']/db_modified['IndustryFraction'].sum()
    #verify_total  = db_modified['IndustryFraction'].sum()
    EconomicResearchFactorsPublish().publish_industry_weights(db_modified, st.session_state[f"{key_prefix}base_year_select_value"], st.session_state[f"{key_prefix}region_select_value"], st.session_state[f"{key_prefix}country_select_value"] )
    st.session_state.selection_committed_weights = False
    st.session_state.show_research_weights = False

def _editor_table_change():
    st.session_state.show_research_weights = True

def display_economic_research():
    #st.session_state.show_research_weights = False
    economic_table, save_button, discard_changes, clean_duplicates = st.columns([3, 1, 1,1])
    with economic_table:
        with st.spinner("Loading data..."):
            economic_factors = Economic_Research_Factors(st.session_state[f"{key_prefix}base_year_select_value"])
            automation_degree = economic_factors.get_automation_degree_country(st.session_state[f"{key_prefix}country_select_value"])
            if automation_degree.duplicated(subset=['BaseYear', 'Region','Country', 'Industry']).any():
                st.write("Duplicates found in table....")
                st.session_state[f"{key_prefix}duplicate_value_flag"] = True
            else:
                st.write("Duplicates NOT Found")
                st.session_state[f"{key_prefix}duplicate_value_flag"] = False
            automation_degree['IndustryFraction'] = automation_degree['IndustryFraction'] * 100.0
            st.write("Automation Degree Factors")
            revised_automation_degree = \
                st.data_editor(
                    automation_degree.head(len(automation_degree)),
                    hide_index=True,
                    num_rows="fixed",
                    on_change= _editor_table_change(),
                    key= "data_editor_key",
                    column_order=("BaseYear","Country", "Industry", "AutomationDegree", "IndustryFraction"),
                    column_config={
                        "BaseYear": st.column_config.NumberColumn(
                            "Year", format="%.0f", min_value=0, max_value=10, width="small"
                        ),
                        "Country": st.column_config.TextColumn("Country", disabled=True, width="medium"),
                        "Industry": st.column_config.TextColumn("Industry", disabled=True, width="medium"),
                        "AutomationDegree": st.column_config.NumberColumn(
                            "Automation Degree", format="%.2f", min_value=0, max_value=10, width="small"
                        ),
                        "IndustryFraction": st.column_config.NumberColumn(
                            "Industry Fraction", format="%.2f%%", min_value=0.0, max_value=100.0, step=0.1, width="small"
                        )
                    }
                )
            st.session_state.automation_degree_table = revised_automation_degree
    with save_button:
        st.button('Save', on_click=save_changes_button)
    with discard_changes:
        st.button('Discard', on_click=discard_changes_button)
    with clean_duplicates:
        if st.session_state[f"{key_prefix}duplicate_value_flag"]:
            st.button('Clean Duplicates', on_click=clean_duplicates_button)
# columns
base_year_selection_col, region_selection_col, country_selection_col, col4 = st.columns(4)
status =st.session_state.show_research_weights
commit_status = st.session_state.selection_committed_weights
if not commit_status:
    # base_year_list = list(get_base_year_list())
    if not base_year_list:
        st.error("No base years available.")
        st.stop()
    # Get region list based on base year
    # region_list = list(EconomicResearchFactorsRanges().get_industry_weights_research_regions(st.session_state.base_year))
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
    # UI layout
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
        country_list = __get_country_list(st.session_state[f"{key_prefix}base_year_select_value"],st.session_state[f"{key_prefix}region_select_value"])
        #list(EconomicResearchFactorsRanges().get_countries_from_industry_weights_research(st.session_state[f"{key_prefix}base_year_select_value"],st.session_state[f"{key_prefix}region_select_value"]))
        if not st.session_state[f"{key_prefix}country_select_value"] in country_list:
            st.session_state[f"{key_prefix}country_select_value"] = country_list[0]
            debug_country = st.session_state[f"{key_prefix}country_select_value"]
        else:
            debug_country = st.session_state[f"{key_prefix}country_select_value"]
        country_index = country_list.index(st.session_state[f"{key_prefix}country_select_value"])
        st.selectbox('Country', country_list, index = country_index, key=f"{key_prefix}country_select_value")

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

with col4:
    if not st.session_state.selection_committed_weights:
        st.button('Display Research Factors', on_click=commit_selection, key = f"{key_prefix}display_research_button")

if st.session_state.show_research_weights:
    display_economic_research()
    # ✅ Now that the table is shown, update previous selections
    st.session_state[f"{key_prefix}prev_base_year"] = st.session_state[f"{key_prefix}base_year_select_value"]
    st.session_state[f"{key_prefix}prev_region"] = st.session_state[f"{key_prefix}region_select_value"]
    st.session_state[f"{key_prefix}prev_country"] = st.session_state[f"{key_prefix}country_select_value"]
    #st.session_state.show_research_weights = False