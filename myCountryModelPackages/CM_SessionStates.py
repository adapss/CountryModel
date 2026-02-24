from myCountryModelPackages.global_constants import *
from myCountryModelPackages.MarketReportRetrieval import *
from myCountryModelPackages.Economic_Research import *
from myCountryModelPackages.ProductTechnologyGroup import *

warnings.filterwarnings('ignore')
_key_prefix = global_session_states_key()

def __is_in_scope_global_session_states():
    if 'db_cxcn_market_research' not in st.session_state or \
            'db_engine_publication' not in st.session_state or \
            'base_year' not in st.session_state or \
            f"{_key_prefix}msal_access_token" not in st.session_state :
        return False
    else:
        return True

def global_session_states_initialize():
    if not __is_in_scope_global_session_states():
        db_connections = DatabaseConnections()
        market_report_db_cxcn = db_connections.get_MiraLite_Connection()
        market_report_db_engine = db_connections.get_MiraLite_engine()

        if 'db_cxcn_market_research' not in st.session_state:
            st.session_state.db_cxcn_market_research = market_report_db_engine #market_report_db_cxcn

        if 'db_engine_publication' not in st.session_state:
            #_db_engine = market_report_db_engine #DatabaseConnections().get_MiraLite_engine()
            st.session_state.db_engine_publication = market_report_db_engine

        if 'base_year' not in st.session_state:
            base_year_list = \
                MarketReports(st.session_state.db_cxcn_market_research).get_base_year_list()
            economic_research = EconomicResearchFactorsRanges()
            base_year_list_economic_research = economic_research.get_economic_research_years()
            intersection_years = set(base_year_list_economic_research) & set(base_year_list)
            st.session_state.base_year = max(intersection_years)
            st.session_state[f"{_key_prefix}economic_model_years"] = intersection_years

        if 'global_session_states_initialized' not in st.session_state:
            st.session_state.global_session_states_initialized = True

        if f"{_key_prefix}msal_access_token" not in st.session_state:
            graph_api = MSGraphTokens()
            access_token = graph_api.generate_access_token()
            st.session_state[f"{_key_prefix}msal_access_token"] = access_token

        st.session_state.global_session_states_initialized = True
    return


class GlobalSessionStates:

    def __init__(self):
        empty = None







