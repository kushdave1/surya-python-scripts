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
# cursor = cnxn.cursor()

# #Sample select query
# cursor.execute("SELECT * FROM dbo.INSURED_COVERAGE;") 

# for key in greg:
#     for i in greg[key]:
#         if key != "vehicles":
#             print(key,i)
#         else:
#             print(greg[key][i][0])







sql_policy = "SELECT * FROM dbo.CS_POLICY_INSURED;"
sql_policy_ref = "SELECT * FROM dbo.POLICY;"
sql_drivers = "SELECT * FROM dbo.CS_DRIVER_DETAIL;"
sql_loss_history = "SELECT * FROM POL.POL_LOSSHISTORY;"
sql_vehicles = "SELECT * FROM dbo.CS_VEHICLE_DETAIL;"
sql_coverage = "SELECT * FROM dbo.INSURED_COVERAGE"
sql_endorsements = "SELECT * FROM POL.TRANS_ENDORSEMENTS"
sql_endorsements_drivers = "SELECT * FROM POL.TRANS_DRIVER_ACTIVITY"
sql_endorsements_contacts = "SELECT * FROM dbo.ECMS_ContactPerson"
sql_endorsements_contacts_name = "SELECT * FROM dbo.ECMS_ContactPersonName"
sql_endorsements_vehicles = "SELECT * FROM POL.TRANS_VEHICLE_ACTIVITY"
sql_endorsements_vehicles_data = "SELECT * FROM POL.POL_VEHICLE"


Data_policy = pd.read_sql(sql_policy, cnxn)
Data_policy_ref = pd.read_sql(sql_policy_ref, cnxn)
Data_drivers = pd.read_sql(sql_drivers, cnxn)
Data_loss_history = pd.read_sql(sql_loss_history, cnxnTwo)
Data_vehicles = pd.read_sql(sql_vehicles, cnxn)
Data_coverage = pd.read_sql(sql_coverage, cnxn)
Data_endorsements = pd.read_sql(sql_endorsements, cnxnTwo)
Data_endorsements_drivers = pd.read_sql(sql_endorsements_drivers, cnxnTwo)
Data_endorsements_contacts = pd.read_sql(sql_endorsements_contacts, cnxnThree)
Data_endorsements_contacts_name = pd.read_sql(sql_endorsements_contacts_name, cnxnThree)
Data_endorsements_vehicles = pd.read_sql(sql_endorsements_vehicles, cnxnTwo)
Data_endorsements_vehicles_data = pd.read_sql(sql_endorsements_vehicles_data, cnxnTwo)


Data_endorsements.to_csv("endorsements.csv")
Data_endorsements_drivers.to_csv("endorsements_drivers.csv")
Data_endorsements_contacts.to_csv("endorsements_contacts.csv")
Data_endorsements_contacts_name.to_csv("endorsements_contacts_name.csv")
Data_endorsements_vehicles.to_csv("endorsements_vehicles.csv")

Data_endorsements_vehicles_data.to_csv("endorsements_vehicles_data.csv")
#Data_drivers.to_csv("drivers_map.csv")
Data_vehicles.to_csv("vehicles_map.csv")

total_policies=[]
Data_coverage.to_csv("coverage.csv")
Data_policy.to_csv("policy.csv")

reconcile = pd.read_excel("kushreport.xlsx")
reconcile_dup = pd.read_excel("kushreport2.xlsx")
reconcile_drivers = pd.read_excel("Driver_report.xlsx")

Data_drivers.to_csv("old_drivers.csv")


Data_drivers["FullName"] = Data_drivers["DRVFIRSTNAME"] + Data_drivers["DRVLASTNAME"]
reconcile_drivers["FullName"] = reconcile_drivers["DRVFIRSTNAME"] + reconcile_drivers["DRVLASTNAME"]
reconcile_drivers_two = reconcile_drivers[["FullName", "DRIVER DOB"]].copy()
Data_drivers = Data_drivers.merge(reconcile_drivers_two, on="FullName")
Data_drivers.to_csv("drivers_map.csv")

endorsements_object = {}

Data_endorsements_drivers = Data_endorsements_drivers.rename(columns={'DriverId': 'DRVVERID'})
Data_endorsements_vehicles = Data_endorsements_vehicles.rename(columns={"VehicleId": "VEHVEHICLEID"})
Data_drivers['DRVVERID'] = Data_drivers['DRVVERID'].astype('int64')

Data_vehicles['VEHVEHICLEID'] = Data_vehicles['VEHVEHICLEID'].astype('int64')


driver_replace_list = ["driverFirstName",
          "driverMiddleName",
          "driverLastName",
          "states",
          "licenseNumber",
          "licenseEffDate",
          "licenseExpDate",
          "driverBirthDate"]

driver_replace_mapping = {
      "driverFirstName": 'DRVFIRSTNAME',
      "driverMiddleName": 'DRIVERMIDDLENAME',
      "driverLastName": 'DRVLASTNAME',
      "states": 'DRVLICSTATE',
      "licenseNumber": 'DRVLICENSENO',
      "licenseEffDate": 'DRVLICEFFDATE',
      "licenseExpDate": 'DRVLICEXPDATE',
      "driverBirthDate":'DRIVER DOB'
}

vehicle_replace_list = ["vin",
          "make",
          "model",
          "modelYear",
          "seating"]

vehicle_replace_mapping = {
      "vin": 'VEHVIN',
      "make": 'VEHMAKE',
      "model": 'VEHMODEL',
      "modelYear": 'VEHYEAR',
      "seating": 'VEHSEATS'
}

Data_drivers_endorsements_full = pd.merge(Data_endorsements_drivers, Data_drivers[['FullName', 'DRVLASTNAME', 'DRVFIRSTNAME', 'DRIVERMIDDLENAME', 'DRVLICENSENO', 'DRVLICSTATE', 'DRVAGE', 'DRVLICEFFDATE', 'DRVLICEXPDATE', 'DRIVER DOB', 'DRVVERID']], on='DRVVERID', how='left')

Data_vehicles_endorsements_full = pd.merge(Data_endorsements_vehicles, Data_vehicles[['VEHVIN', 'VEHYEAR',	'VEHMAKE',	'VEHMODEL', 'VEHSEATS', 'VEHICLE_AGE', 'VEHVEHICLEID']], on="VEHVEHICLEID", how='left')

Activity_type = ['REP', 'ADD', 'DEL']

Data_drivers_endorsements_full.to_csv("fulldrivers.csv")

for i in policyData:
      
      policyNum = i['policy']['policyNum']
      
      if policyNum != '22CAN00216':
            continue
      
      policyEffDate = i['policy']['effectiveDate']
      policyExpDate = i['policy']['expirationDate']
      endorsements_object[policyNum] = {}
      
      filtered_df_drivers = Data_drivers_endorsements_full[Data_drivers_endorsements_full['PolicyNumber'].str.replace(' ', '') == policyNum]
      # if policyNum == '21NJT00129':
      #       print(filtered_df_drivers)
      unique_drivers = []
      unique_drivers_df = pd.DataFrame()
      
      print(filtered_df_drivers, 'slal')
      
      for idx, row in filtered_df_drivers.iterrows():
            if row['FullName'] not in unique_drivers:
                  unique_drivers.append(row['FullName'])
                  unique_drivers_df = pd.concat([unique_drivers_df, row.to_frame().T])
      
      
      if len(unique_drivers_df) > 0:
            unique_drivers_df['time_diff'] = (unique_drivers_df['ExpirationDate'] - unique_drivers_df['EffectiveDate']).dt.days

            # Filter out rows where time_diff is exactly 365 days
            unique_drivers_df = unique_drivers_df[unique_drivers_df['time_diff'] != 365]

            # Remove the time_diff column
            unique_drivers_df = unique_drivers_df.drop(columns=['time_diff'])
            # if policyNum == '21NJT00129':
            #       print(unique_drivers_df)
            
            for date in unique_drivers_df['ModifiedDate'].unique():
                  date_df = unique_drivers_df[unique_drivers_df['ModifiedDate'] == date]
               
                  effective_date_end = date_df['EffectiveDate'].unique()[0]
                  expiration_date_end = date_df['ExpirationDate'].unique()[0]
                  
                  effDate = np.datetime_as_string(effective_date_end, unit='D')
                  effDateFinal = effDate[5:7] + '/' + effDate[8:10] + '/' + effDate[:4]
                  
                  expDate = np.datetime_as_string(expiration_date_end, unit='D')
                  expDateFinal = expDate[5:7] + '/' + expDate[8:10] + '/' + expDate[:4]
                  
                  
                  if 'REP' in date_df['DriverActivityCode'].values:
                        
                        for k, row in date_df.iterrows():
                              OldDriverId = int(row['OldDriverId'])
                              
                              
                              Data_drivers_old_driver = Data_drivers[Data_drivers['DRVVERID'] == OldDriverId].head(1)
                              
                              old_driver_first_name = Data_drivers_old_driver['DRVFIRSTNAME'].iloc[0]
                              old_driver_last_name = Data_drivers_old_driver['DRVLASTNAME'].iloc[0]
                              
      
                              #old_driver_middle_name = Data_drivers_old_driver['DRVMIDDLENAME']
                              # old_driver_license_state = Data_drivers_old_driver['DRVLICSTATE']
                              # old_driver_license_num = Data_drivers_old_driver['DRVLICENSENO']
                              # old_driver_license_effDate = Data_drivers_old_driver['DRVLICEFFDATE']
                              # old_driver_license_expDate = Data_drivers_old_driver['DRVLICEXPDATE']
                              # old_driver_dob = Data_drivers_old_driver['DRIVER DOB']
                              
                              for j in i['drivers']['values']:
                                    if j['driverFirstName'] == old_driver_first_name and j['driverLastName'] == old_driver_last_name:
                                          index = i['drivers']['values'].index(j)
                                          for d_value in driver_replace_list:
                                                
                                                driver_index = "drivers.values[{0}].{1}".format(index, d_value)
                                                
                                                
                                                if d_value == 'licenseEffDate' or d_value == 'licenseExpDate' or d_value == 'driverBirthDate':
                                                      newVal = row[driver_replace_mapping[d_value]].strftime('%m/%d/%Y')
                                                else:
                                                      newVal = row[driver_replace_mapping[d_value]]
                                                if driver_index in endorsements_object[policyNum]:
                                                      endorsements_object[policyNum][driver_index]['values'].append({
                                                            'oldValue': j[d_value],
                                                            'newValue': newVal,
                                                            'time': str(date),
                                                            'effDate': effDateFinal
                                                      })
                                                else:
                                                      endorsements_object[policyNum][driver_index] = {'values': [{
                                                            'oldValue': j[d_value],
                                                            'newValue': newVal,
                                                            'time': str(date),
                                                            'effDate': effDateFinal
                                                      }]}
                              
                                                            

                              
                              
                              
                              
                              
                        
                  elif 'ADD' in date_df['DriverActivityCode'].values:
                        relevant_drivers_old = []
                        relevant_drivers_new = []
                              # Get the relevant rows from policyJSON
                        for j in i['drivers']['values']:
                              if np.datetime64(datetime.datetime.strptime(j['driverEffDate'], '%m/%d/%Y')) < effective_date_end:
                                    relevant_drivers_old.append(j)
                        
                        for j in i['drivers']['values']:
                              if np.datetime64(datetime.datetime.strptime(j['driverEffDate'], '%m/%d/%Y')) <= effective_date_end:
                                    relevant_drivers_new.append(j)
                                    
                        if 'drivers.values' in endorsements_object[policyNum] and 'values' in endorsements_object[policyNum]['drivers.values']:
                              endorsements_object[policyNum]['drivers.values']['values'].append({
                                    'oldValue': relevant_drivers_old,
                                    'newValue': relevant_drivers_new,
                                    'time': str(date),
                                    'effDate': effDateFinal
                              })
                        else:
                              endorsements_object[policyNum]['drivers.values'] = {'values': [{
                                    'oldValue': relevant_drivers_old,
                                    'newValue': relevant_drivers_new,
                                    'time': str(date),
                                    'effDate': effDateFinal
                              }]}
                        
                        
                        
                        
                  elif 'DEL' in date_df['DriverActivityCode'].values:
                        
                        drivers_deleted = []
                        
                        for j in i['drivers']['values']:
                              if np.datetime64(datetime.datetime.strptime(j['driverExpDate'], '%m/%d/%Y')) == expiration_date_end:
                                    index = i['drivers']['values'].index(j)
                                    driver_index = "drivers.values[{}].driverExpDate".format(index)
                                    drivers_deleted.append(driver_index)
                        
                        for driver in drivers_deleted:
                              if driver in endorsements_object[policyNum] and 'values' in endorsements_object[policyNum][driver]:
                                    endorsements_object[policyNum][driver]['values'].append({
                                          'oldValue': policyExpDate,
                                          'newValue': expDateFinal,
                                          'time': str(date),
                                          'effDate': expDateFinal
                                    })
                              else:
                                    endorsements_object[policyNum][driver] = {'values' : [{
                                          'oldValue': policyExpDate,
                                          'newValue': expDateFinal,
                                          'time': str(date),
                                          'effDate': expDateFinal
                                    }]}
      
      filtered_df_vehicles = Data_vehicles_endorsements_full[Data_vehicles_endorsements_full['PolicyNumber'].str.replace(' ', '') == policyNum]
      filtered_df_vehicles = filtered_df_vehicles[~filtered_df_vehicles['VehicleActivityCode'].isin(['EDIT', 'RCH', 'NEW', ''])]
      
      unique_vehicles = []
      unique_vehicles_df = pd.DataFrame()
      
      for idx, row in filtered_df_vehicles.iterrows():
            if row['VEHVIN'] not in unique_vehicles:
                  unique_vehicles.append(row['VEHVIN'])
                  unique_vehicles_df = pd.concat([unique_vehicles_df, row.to_frame().T])
      
      
      if len(unique_vehicles_df) > 0:
            unique_vehicles_df['time_diff'] = (unique_vehicles_df['ExpirationDate'] - unique_vehicles_df['EffectiveDate']).dt.days

            # Filter out rows where time_diff is exactly 365 days
            unique_vehicles_df = unique_vehicles_df[unique_vehicles_df['time_diff'] != 365]

            # Remove the time_diff column
            unique_vehicles_df = unique_vehicles_df.drop(columns=['time_diff'])
            
            for date in unique_vehicles_df['ModifiedDate'].unique():
                  date_df = unique_vehicles_df[unique_vehicles_df['ModifiedDate'] == date]


        
               
                  effective_date_end = date_df['EffectiveDate'].unique()[0]
                  expiration_date_end = date_df['ExpirationDate'].unique()[0]
                  
                  effDate = np.datetime_as_string(effective_date_end, unit='D')
                  effDateFinal = effDate[5:7] + '/' + effDate[8:10] + '/' + effDate[:4]
                  
                  expDate = np.datetime_as_string(expiration_date_end, unit='D')
                  expDateFinal = expDate[5:7] + '/' + expDate[8:10] + '/' + expDate[:4]
                  
                  # for Activity in Activity_type:
                  #       date_df = date_df[date_df['VehicleActivityCode']==Activity]
                  #       print(date_df, Activity)
                  if 'REP' in date_df['VehicleActivityCode'].values:
   
                        date_df = date_df[date_df['VehicleActivityCode']=='REP']
                        for k, row in date_df.iterrows():
                              OldVehicleId = int(row['OldVehicleId'])
                              
                              
                              Data_vehicles_old_vehicles = Data_vehicles[Data_vehicles['VEHVEHICLEID'] == OldVehicleId].head(1)
                              
                              
                              if len(Data_vehicles_old_vehicles) > 0:

                                    old_vehicle_vin = Data_vehicles_old_vehicles['VEHVIN'].iloc[0]
                                    

                                    #old_driver_middle_name = Data_drivers_old_driver['DRVMIDDLENAME']
                                    # old_driver_license_state = Data_drivers_old_driver['DRVLICSTATE']
                                    # old_driver_license_num = Data_drivers_old_driver['DRVLICENSENO']
                                    # old_driver_license_effDate = Data_drivers_old_driver['DRVLICEFFDATE']
                                    # old_driver_license_expDate = Data_drivers_old_driver['DRVLICEXPDATE']
                                    # old_driver_dob = Data_drivers_old_driver['DRIVER DOB']
                                    
                                    for j in i['vehicles']['values']:
                                          if j['vin'] == old_vehicle_vin:
                                                index = i['vehicles']['values'].index(j)
                                                for d_value in vehicle_replace_list:
                                                      
                                                      vehicle_index = "vehicles.values[{0}].{1}".format(index, d_value)
                                    
                                                      if vehicle_index in endorsements_object[policyNum]:
                                                            endorsements_object[policyNum][vehicle_index]['values'].append({
                                                                  'oldValue': j[d_value],
                                                                  'newValue': row[vehicle_replace_mapping[d_value]],
                                                                  'time': str(date),
                                                                  'effDate': effDateFinal
                                                            })
                                                      else:
                                                            endorsements_object[policyNum][vehicle_index] = {'values': [{
                                                                  'oldValue': j[d_value],
                                                                  'newValue': row[vehicle_replace_mapping[d_value]],
                                                                  'time': str(date),
                                                                  'effDate': effDateFinal
                                                            }]}
                                    
                                                                  

                              
                              
                              
                              
                              
                        
                  elif 'ADD' in date_df['VehicleActivityCode'].values:
                        print(date_df, policyNum, "ADD")
                        relevant_vehicles_old = []
                        relevant_vehicles_new = []
                              # Get the relevant rows from policyJSON
                        for j in i['vehicles']['values']:
                              if np.datetime64(datetime.datetime.strptime(j['baseEffDate'], '%m/%d/%Y')) < effective_date_end:
                                    relevant_vehicles_old.append(j)
                        
                        for j in i['vehicles']['values']:
                              if np.datetime64(datetime.datetime.strptime(j['baseEffDate'], '%m/%d/%Y')) <= effective_date_end:
                                    relevant_vehicles_new.append(j)
                                    
                        if 'vehicles.values' in endorsements_object[policyNum] and 'values' in endorsements_object[policyNum]['vehicles.values']:
                              endorsements_object[policyNum]['vehicles.values']['values'].append({
                                    'oldValue': relevant_vehicles_old,
                                    'newValue': relevant_vehicles_new,
                                    'time': str(date),
                                    'effDate': effDateFinal
                              })
                        else:
                              endorsements_object[policyNum]['vehicles.values'] = {'values': [{
                                    'oldValue': relevant_vehicles_old,
                                    'newValue': relevant_vehicles_new,
                                    'time': str(date),
                                    'effDate': effDateFinal
                              }]}
                        
                        
                        
                        
                  elif 'DEL' in date_df['VehicleActivityCode'].values:
                        
                        vehicles_deleted = []
                        
                        for j in i['vehicles']['values']:
                              if np.datetime64(datetime.datetime.strptime(j['baseExpDate'], '%m/%d/%Y')) == expiration_date_end:
                                    index = i['vehicles']['values'].index(j)
                                    vehicle_index = "vehicles.values[{}].baseExpDate".format(index)
                                    vehicles_deleted.append(vehicle_index)
                        
                        for vehicle in vehicles_deleted:
                              if vehicle in endorsements_object[policyNum] and 'values' in endorsements_object[policyNum][vehicle]:
                                    endorsements_object[policyNum][vehicle]['values'].append({
                                          'oldValue': policyExpDate,
                                          'newValue': expDateFinal,
                                          'time': str(date),
                                          'effDate': expDateFinal
                                    })
                              else:
                                    endorsements_object[policyNum][vehicle] = {'values' : [{
                                          'oldValue': policyExpDate,
                                          'newValue': expDateFinal,
                                          'time': str(date),
                                          'effDate': expDateFinal
                                    }]}           
                        
                              
                        
                  # date = pd.to_datetime()

                  # # Format datetime object as string
                  # date_str = date.strftime('%m/%d/%Y %H:%M:%S')


with open('endorsements.txt', 'w') as f:
      f.write(str(endorsements_object))


      # if 'REP' in unique_drivers_df['DriverActivityCode'].values:
      #       rep_count = (unique_drivers_df['DriverActivityCode'] == 'REP').sum()
      #       print(f"REPLACE IS PRESENT. REP count: {rep_count} {policyNum}")
      # elif 'ADD' in unique_drivers_df['DriverActivityCode'].values:
      #       add_count = (unique_drivers_df['DriverActivityCode'] == 'ADD').sum()
      #       print(f"Only ADD is present. ADD count: {add_count} {policyNum}")
      #       print(unique_drivers_df)
      # elif 'DELETE' in unique_drivers_df['DriverActivityCode'].values:
      #       delete_count = (unique_drivers_df['DriverActivityCode'] == 'DEL').sum()
      #       print(f"Only DELETE is present. DELETE count: {delete_count} {policyNum}")
      # else:
      #       print("Neither ADD nor DELETE is present")
            
      
