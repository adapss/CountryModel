import sys
import warnings
import pyodbc
import pandas as pd
import numpy as np

warnings.filterwarnings('ignore')

class sqlConnection:
     connection = None

     def get_connection(self):
         return self.connection

     def __init__(self):
         server = 'dh4s5e92kd.database.windows.net'
         database = 'MiraLite'
         username = 'ARCSQLAdmin'
         password = '{Reverb19!}'
         # driver = '{ODBC Driver 17 for SQL Server}'
         driver = '{ODBC Driver 18 for SQL Server}'
         self.connection = pyodbc.connect(
             'DRIVER=' + driver + ';SERVER=' + server + ';DATABASE=' + database + ';UID=' + username + ';PWD=' + password)
         #self.connection = pyodbc.connect('DRIVER=' + '{ODBC Driver 18 for SQL Server}'+ ';SERVER=' + 'dh4s5e92kd.database.windows.net' + ';DATABASE=' + 'MiraLite' + ';UID=' + 'ARCSQLAdmin' + ';PWD=' + '{Reverb19!}')






