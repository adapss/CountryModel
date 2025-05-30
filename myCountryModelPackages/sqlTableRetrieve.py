import sys
import warnings
import pyodbc
from sqlalchemy import create_engine
import pandas as pd
import numpy as np

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


class sql_MiraIndustry_Connection:
    connection = None
    connection = None
    server = 'dh4s5e92kd.database.windows.net'
    database = 'MiraIndustry'
    username = 'ARCSQLAdmin'
    password = '{Reverb19!}'
    driver = '{ODBC Driver 18 for SQL Server}'

    def get_connection(self):
        return self.connection

    def __init__(self):
        self.server = 'dh4s5e92kd.database.windows.net'
        self.database = 'MiraIndustry'
        self.username = 'ARCSQLAdmin'
        self.password = '{Reverb19!}'
        self.driver = '{ODBC Driver 18 for SQL Server}'
        self.connection = pyodbc.connect(
            'DRIVER=' + self.driver + ';SERVER=' + self.server + ';DATABASE=' + self.database + ';UID=' + self.username + ';PWD=' + self.password)


class sql_MiraLite_connection:
     connection = None
     connection = None
     server = 'dh4s5e92kd.database.windows.net'
     database = 'MiraLite'
     username = 'ARCSQLAdmin'
     password = '{Reverb19!}'
     driver = '{ODBC Driver 18 for SQL Server}'

     def get_connection(self):
         return self.connection

     def __init__(self):
        self.server = 'dh4s5e92kd.database.windows.net'
        self.database = 'MiraLite'
        self.username = 'ARCSQLAdmin'
        self.password = '{Reverb19!}'
        self.driver = '{ODBC Driver 18 for SQL Server}'
        self.connection = pyodbc.connect(
            'DRIVER=' + self.driver + ';SERVER=' + self.server + ';DATABASE=' + self.database + ';UID=' + self.username + ';PWD=' + self.password)


class sql_MiraWIP_Connection:
    connection = None
    server = 'dh4s5e92kd.database.windows.net'
    database = 'MiraWIP'
    username = 'ARCSQLAdmin'
    password = '{Reverb19!}'
    driver = '{{ODBC Driver 18 for SQL Server}}'

    def get_connection(self):
        return self.connection

    def get_engine(self):
        _password = 'Reverb19!'
        _driver = 'ODBC Driver 18 for SQL Server'
        connection_string = f'mssql+pyodbc://{self.username}:{_password}@{self.server}/{self.database}?driver={_driver}'
        try:
            engine = create_engine(connection_string)
            with engine.connect() as connection:
                print("Connection successful!")
            return engine
        except Exception as e:
            print(f"Error connecting to the database: {e}")
            return None

    def __init__(self):
        self.server = 'dh4s5e92kd.database.windows.net'
        self.database = 'MiraWIP'
        self.username = 'ARCSQLAdmin'
        self.password = '{Reverb19!}'
        self.driver = '{ODBC Driver 18 for SQL Server}'
        self.connection = pyodbc.connect(
            'DRIVER=' + self.driver + ';SERVER=' + self.server + ';DATABASE=' + self.database + ';UID=' + self.username + ';PWD=' + self.password)




