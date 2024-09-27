import pyodbc 
import pandas as pd
import ast
from datetime import timedelta
import datetime
import numpy as np
# Some other example server values are
# server = 'localhost\sqlexpress' # for a named instance
# server = 'myserver,port' # to specify an alternate port

#################################################################
#################################################################
######################### ENDORSEMENTS ##########################
#################################################################
#################################################################


import json
# read contents of file into a string
import requests
policyData=[]
with open("policies.txt", "r") as f:
    text = f.read()
    for i in ast.literal_eval(text):
        policyData.append(i)

server = '38.129.107.12' 
database = 'GREENMILE_POLICY_ADMIN' 
databaseTwo = 'POLICY_ADMIN'
databaseThree = 'ECMS'
username = 'elpisadmin' 
password = 'elpis!@#' 

cnxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)

cnxnTwo = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+databaseTwo+';UID='+username+';PWD='+ password)

cnxnThree = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+databaseThree+';UID='+username+';PWD='+ password)

sql_policy = "SELECT * FROM POL.POL_ADDITIONAL_INTEREST_DETAIL;"


sql_activity = "SELECT * FROM POL.POL_POLICY;"


Data_policy = pd.read_sql(sql_policy, cnxnTwo)
Data_policy.to_csv("addtl_interest.csv")

Data_activity = pd.read_sql(sql_activity, cnxnTwo)
Data_activity.to_csv("addtl_activity.csv")