#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Jun 27 10:09:02 2022

@author: kushdave
"""
import pyodbc 
import sqlserverport

# test data
server_name = "38.129.107.12"
instance_name = "SURYAPROD\SQLEXPRESS01"

try:
    result = r"Instance {0}\{1} is listening on port {2}.".format(
        server_name,
        instance_name,
        sqlserverport.lookup(server_name, instance_name),
    )
except sqlserverport.BrowserError as err:
    result = err.message
except sqlserverport.NoTcpError as err:
    result = err.message

print(result)

server = 'SURYAPROD\SQLEXPRESS01' 
database = 'GREENMILE_POLICY_ADMIN' 
username = 'elpisadmin' 
password = 'elpis!@#' 

cnxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+',1433;DATABASE='+database+';UID='+username+';PWD='+ password, autocommit=True)
cursor = cnxn.cursor()