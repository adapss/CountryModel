import warnings
import pyodbc
from sqlalchemy import create_engine
import streamlit as st
from myCountryModelPackages.sqlTableRetrieve import *
from myCountryModelPackages.MarketReportRetrieval import *

warnings.filterwarnings('ignore')


def __is_in_scope_global_session_states():
    if 'db_cxcn_market_research' not in st.session_state or 'db_engine_publication' not in st.session_state or 'base_year' not in st.session_state:
        return False
    else:
        return True

def initialize_global_session_states():
    if not __is_in_scope_global_session_states():
        market_report_db_cxcn = DatabaseConnections().get_MiraLite_Connection()
        if 'db_cxcn_market_research' not in st.session_state:
            st.session_state.db_cxcn_market_research = market_report_db_cxcn
        if 'db_engine_publication' not in st.session_state:
            _db_engine = DatabaseConnections().get_MiraLite_engine()
            st.session_state.db_engine_publication = _db_engine
        if 'base_year' not in st.session_state:
            base_year_list = MarketReports(
            st.session_state.db_cxcn_market_research).get_base_year_list().sort_values(
            ascending=False)
            st.session_state.base_year = base_year_list.max()
        if 'global_session_states_initialized' not in st.session_state:
            st.session_state.global_session_states_initialized = True
        st.session_state.global_session_states_initialized = True
    return


class GlobalSessionStates:

    def __init__(self):
        empty = None

# Database Connections and Engines
#
class DatabaseConnections:
    server = 'dh4s5e92kd.database.windows.net'
    username = 'ARCSQLAdmin'
    password = '{Reverb19!}'
    driver = '{ODBC Driver 18 for SQL Server}'
    _password = 'Reverb19!'
    _driver = 'ODBC Driver 18 for SQL Server'

    # Connections used with Pandas Library
    def __get_connection(self,data_base):
        cxcn = pyodbc.connect(
            'DRIVER=' + self.driver + ';SERVER=' + self.server + ';DATABASE=' + data_base + ';UID=' + self.username + ';PWD=' + self.password)
        return cxcn

    # Connection Used with sqlAlchemey Library
    def __get_engine(self, data_base):
        connection_string = f'mssql+pyodbc://{self.username}:{self._password}@{self.server}/{data_base}?driver={self._driver}'
        try:
            engine = create_engine(connection_string)
            with engine.connect() as connection:
                print("Connection successful!")
            return engine
        except Exception as e:
            print(f"Error connecting to the database: {e}")
            return None

    def get_MiraIndustry_Connection(self):
        database = 'MiraIndustry'
        return self.__get_connection(database)

    def get_MiraLite_Connection(self):
        database = 'MiraLite'
        return self.__get_connection(database)

    def get_MiraWIP_Connection(self):
        return self.__get_connection('MiraWIP')

    def get_MiraWIP_engine(self):
        return self.__get_engine('MiraWIP')

    def get_MiraLite_engine(self):
        return self.__get_engine('MiraLite')

    def get_MiraIndustry_engine(self):
        return self.__get_engine('MiraIndustry')

    def __init__(self):
        empty = None





