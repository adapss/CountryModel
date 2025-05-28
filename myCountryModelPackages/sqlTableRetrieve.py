import sys
import warnings
import pyodbc
from sqlalchemy import create_engine
import pandas as pd
import numpy as np

warnings.filterwarnings('ignore')


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




