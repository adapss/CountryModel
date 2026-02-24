import warnings
import pyodbc
from sqlalchemy import create_engine
from urllib.parse import quote_plus

warnings.filterwarnings('ignore')


# Database Connections and Engines
#
class DatabaseConnections:
    server = 'dh4s5e92kd.database.windows.net'
    username = 'ARCSQLAdmin'
    password = '{Reverb19!}'
    driver = '{ODBC Driver 18 for SQL Server}'
    _password = 'Reverb19!'
    _driver = 'ODBC Driver 18 for SQL Server'

    def __init__(self):
        empty = None

    # Connections used with Pandas Library
#    def __get_connection(self,data_base):
#       cxcn = pyodbc.connect(
#           'DRIVER=' + self.driver + ';SERVER=' + self.server + ';DATABASE=' + data_base + ';UID=' + self.username + ';PWD=' + self.password)
#       return cxcn
#
#   # Connection Used with sqlAlchemey Library
#   def __get_engine(self, data_base):
#       connection_string = f'mssql+pyodbc://{self.username}:{self._password}@{self.server}/{data_base}?driver={self._driver}'
#       try:
#           engine = create_engine(connection_string)
#           with engine.connect() as connection:
#               print("Connection successful!")
#           return engine
#       except Exception as e:
#           print(f"Error connecting to the database: {e}")
#           return None

    def __get_engine(self, database: str):
        odbc_params = (
            f"DRIVER={self.driver};"  # e.g., {ODBC Driver 18 for SQL Server}
            f"SERVER={self.server};"
            f"DATABASE={database};"
            f"UID={self.username};PWD={self._password};"
            "Encrypt=yes;TrustServerCertificate=yes;"  # adjust per your security policy
        )
        # Correct: use urllib.parse.quote_plus, not pyodbc.quote_plus
        odbc_connect = quote_plus(odbc_params)

        engine = create_engine(
            f"mssql+pyodbc:///?odbc_connect={odbc_connect}",
            pool_pre_ping=True,  # validates/replaces dead conns before each use
            pool_recycle=1800,  # recycles connections older than 30 minutes
            future=True,
        )
        return engine

    def __get_connection(self, database):
        # Use only for libraries that require DB-API (not Pandas to_sql)
        return pyodbc.connect(
            f"DRIVER={self.driver};SERVER={self.server};DATABASE={database};UID={self.username};PWD={self._password};TrustServerCertificate=yes;"
        )

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







