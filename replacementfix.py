from pymongo import MongoClient
import json
import pandas as pd
import math
import datetime

# Connect to the MongoDB instance
client = MongoClient('mongodb+srv://Chandra99:Meteor11@suryasystems.zx4gl.mongodb.net/?retryWrites=true&w=majority')

# Get the database you want to work with
db = client['test']

# Get the collection you want to filter
collection = db['Policies']
endorsements = db['Endorsements']

# Define your filter criteria
filter_criteria = {}

# Query the collection using the filter criteria
results = collection.find()

driverreport = pd.read_excel("Final_driver_report_missingcheck.xlsx")


# Loop through the results and print them
for result in results:
    policy_json = json.loads(result['policyJson'])
    policyNum = policy_json['policy']['policyNum']
    
    driverReportPolicy = driverreport[driverreport['DRVPOLICYNO'] == policyNum]
    
    limits = [   'combinedSectionLimit',  'splitSectionBodyPerPerson',    'splitSectionBodyPerAccidentOptions',    'splitSectionPropertyDamageOptions',    'pIProtectionSingleLimit',    'pIProtectionSplitBodyPerPerson',    'pIProtectionSplitBodyPerAccident',    'pIProtectionSplitPropertyDamage',    'medicalSingleLimit',    'medicalSplitBodyPerPerson',    'medicalSplitBodyPerAccident',    'medicalSplitPropertyDamage',   'underinsuredMotoristSingleLimit',    'underMotoristBodyPerPerson',    'underMotoristBodyPerAccident',    'underMotoristProperty',     'unMotoristBodyPerAccident',    'unMotoristBodyPerPerson',    'unMotoristProperty',    'uninsuredMotoristSingleLimit']
    premiums = ['overallPremium',
        'personalInjuryProtectionPremium',
        'pedPipProtectionPremium',
        'medicalPaymentsPremium',
        'underinsuredMotoristPremium',
        'uninsuredMotoristPremium']
    filter_criteria = {'policyNum': policyNum}
    end_result = endorsements.find(filter_criteria)
    
    # if policyNum != '20NJN00091':
    #     continue
    
    # if policyNum != '23NJN00217':
    #     continue
    
    
    try:
        endorsements_json = json.loads(end_result[0]['endorsementsJson'])
    except:
        continue
    
    
    
    
    # for i in policy_json['drivers']['values']:
    #     if i['driverFirstName'].isin(driverReportPolicy['DRVFIRSTNAME']) and i['driverLastName'].isin(driverReportPolicy['DRVLASTNAME']):
    #         print(i)
    def has_letter(str):
        return any(c.isalpha() for c in str)
    
    
    # for i in limits:
    #     number_string = policy_json['coverage'][i]
    #     if number_string != 'nan':
            
    #         if has_letter(str(number_string)):
    #             continue
    #         else:
    #             policy_json['coverage'][i] = "{:,}".format(int(str(number_string).replace(',', '').split('.')[0]))

    #             print(i, "{:,}".format(int(str(number_string).replace(',', '').split('.')[0])), policy_json['coverage'], policyNum)
    
    # combined_limit = policy_json['coverage']['combinedSectionLimit']
    # accident_limit = policy_json['coverage']['splitSectionBodyPerAccidentOptions']
    
    # if ',' in combined_limit:
    #     policy_json['coverage']['overall'] = "Combined Single Limit"
    #     policy_json['coverage']['combinedSectionEntry'] = "17"
    #     policy_json['coverage']['splitSectionAutoEntryOptions'] = 'Excluded'
    # else:
    #     policy_json['coverage']['overall'] = "Split Limit"
    #     policy_json['coverage']['combinedSectionEntry'] = 'Excluded'
    #     policy_json['coverage']['splitSectionAutoEntryOptions'] = "17"
        
    
    drivers = policy_json['drivers']['values']
    
    vehicles = policy_json['vehicles']['values']
    driverFirstNames = []
    driverLastNames = []
    
    def format_date(date_str):
        # Parse the input date string as a datetime object
        date_obj = datetime.datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S')
        
        # Format the date object as a string with the desired format
        formatted_date = date_obj.strftime('%m/%d/%Y')
        
        return formatted_date
    
    # if policyNum != '23CAN00301':
    #     continue
    
    endorsement_keys = list(endorsements_json.keys())
    
    print(endorsements_json)
    for index, vehicle in enumerate(vehicles):
        
        # for index2, veh in enumerate(vehicles):
        #     if vehicle['vin'] == veh['vin'] and index != index2:
        #         if vehicle['baseEffDate'] != policy_json['policy']['effectiveDate'] or vehicle['baseExpDate'] != policy_json['policy']['expirationDate'] and veh['baseEffDate'] == policy_json['policy']['effectiveDate'] and veh['baseExpDate'] == policy_json['policy']['expirationDate']:
        #             for i in premiums:
        #                 veh[i] = 0.00    
                    
            
        #             for i in endorsement_keys:
        #                 if str(index2) in i and 'vehicle' in i:
        #                     print(i)
        #                     del endorsements_json[i]         
        
        string_index = "vehicles.values[%s].vin" % index
        
        exp_index = "vehicles.values[%s].baseExpDate" % index
        
        
        if vehicle['baseEffDate'] != policy_json['policy']['effectiveDate']:
            
            for i in endorsement_keys:
                try:
                    if endorsements_json[i]['values'][0]['newValue'] == vehicle['vin']:
                    
                    
                        vehicle_end = endorsements_json['vehicles.values']['values']
                        for j in vehicle_end:
                            old_value = j['oldValue']
                            new_value = j['newValue']
                            
                            
                            if old_value is not None and new_value is not None:
                                new_values_only = [item for item in new_value if item not in old_value]
                            
                                for k in new_values_only:
                                    if k['vin'] == vehicle['vin'] and len(new_values_only) > 1:
                                        new_value.remove(k)
                                    elif k['vin'] == vehicle['vin'] and len(new_values_only) == 1:
                                        vehicle_end.remove(j)
                            
                except:
                    continue
        
        
        if vehicle['baseExpDate'] != policy_json['policy']['expirationDate']:
            

            try:
                if exp_index in endorsement_keys and string_index in endorsement_keys:
                    
                    if endorsements_json[exp_index]['values'][0]['newValue'] == vehicle['baseExpDate'] and endorsements_json[string_index]['values'][0]['oldValue'] == vehicle['vin']:
                        
                        del endorsements_json[exp_index]
                        
                        # vehicle_end = endorsements_json['vehicles.values']['values']
                        # for j in vehicle_end:
                        #     old_value = j['oldValue']
                        #     new_value = j['newValue']
                            
                            
                        #     if old_value is not None and new_value is not None:
                        #         new_values_only = [item for item in new_value if item not in old_value]
                            
                        #         for k in new_values_only:
                        #             if k['vin'] == vehicle['vin'] and len(new_values_only) > 1:
                        #                 new_value.remove(k)
                        #             elif k['vin'] == vehicle['vin'] and len(new_values_only) == 1:
                        #                 vehicle_end.remove(j)
                        
            except:
                continue
            
    # for index, driver in enumerate(drivers):
        
    #     for index2, dr in enumerate(drivers):
    #         if driver['driverFirstName'] == dr['driverFirstName'] and driver['driverLastName'] == dr['driverLastName'] and index != index2:
    #             # if driver['driverEffDate'] != policy_json['policy']['effectiveDate'] or driver['driverExpDate'] != policy_json['policy']['expirationDate'] and veh['baseEffDate'] == policy_json['policy']['effectiveDate'] and veh['baseExpDate'] == policy_json['policy']['expirationDate']:
    #             #     for i in premiums:
    #             #         veh[i] = 0.00    
                    
            
    #             #     for i in endorsement_keys:
    #             #         if str(index2) in i and 'vehicle' in i:
    #             #             print(i)
    #             #             del endorsements_json[i]         
        
    #     string_index = "drivers.values[%s].vin" % index
        
    #     exp_index = "vehicles.values[%s].baseExpDate" % index
        
        
    #     if vehicle['baseEffDate'] != policy_json['policy']['effectiveDate']:
            
    #         for i in endorsement_keys:
    #             try:
    #                 if endorsements_json[i]['values'][0]['newValue'] == vehicle['vin']:
                    
                    
    #                     vehicle_end = endorsements_json['vehicles.values']['values']
    #                     for j in vehicle_end:
    #                         old_value = j['oldValue']
    #                         new_value = j['newValue']
                            
                            
    #                         if old_value is not None and new_value is not None:
    #                             new_values_only = [item for item in new_value if item not in old_value]
                            
    #                             for k in new_values_only:
    #                                 if k['vin'] == vehicle['vin'] and len(new_values_only) > 1:
    #                                     new_value.remove(k)
    #                                 elif k['vin'] == vehicle['vin'] and len(new_values_only) == 1:
    #                                     vehicle_end.remove(j)
                            
    #             except:
    #                 continue
        
        
    #     if vehicle['baseExpDate'] != policy_json['policy']['expirationDate']:
            

    #         try:
    #             if exp_index in endorsement_keys and string_index in endorsement_keys:
                    
    #                 if endorsements_json[exp_index]['values'][0]['newValue'] == vehicle['baseExpDate'] and endorsements_json[string_index]['values'][0]['oldValue'] == vehicle['vin']:
                        
    #                     del endorsements_json[exp_index]
                        
    #                     # vehicle_end = endorsements_json['vehicles.values']['values']
    #                     # for j in vehicle_end:
    #                     #     old_value = j['oldValue']
    #                     #     new_value = j['newValue']
                            
                            
    #                     #     if old_value is not None and new_value is not None:
    #                     #         new_values_only = [item for item in new_value if item not in old_value]
                            
    #                     #         for k in new_values_only:
    #                     #             if k['vin'] == vehicle['vin'] and len(new_values_only) > 1:
    #                     #                 new_value.remove(k)
    #                     #             elif k['vin'] == vehicle['vin'] and len(new_values_only) == 1:
    #                     #                 vehicle_end.remove(j)
                        
    #         except:
    #             continue
    
    print(endorsements_json)     

            
        
    
    # for index1, driver1 in enumerate(drivers):
    #     try:
            
    #         if driver1['driverMiddleName'] == 'nan':
    #             driver1['driverMiddleName'] = ''
    #     except:
    #         print('hi')
        
        # for index2, driver2 in enumerate(drivers):
        #     print(index1, index2)
        #     if index1 != index2:
        #         print(len(driver1['driverFirstName']), len(driver2['driverFirstName']), driver1['driverFirstName'].strip() == driver2['driverFirstName'].strip() and driver1['driverLastName'].strip() == driver2['driverLastName'].strip() and driver1['driverBirthDate'].strip() == driver2['driverBirthDate'].strip())
        #         if driver1['driverFirstName'].strip() == driver2['driverFirstName'].strip() and driver1['driverLastName'].strip() == driver2['driverLastName'].strip() and driver1['driverBirthDate'].strip() == driver2['driverBirthDate'].strip():
        #             print(driver1, driver2)
        #             drivers.remove(driver2)
        
    #     # if '-' in i['driverEffDate']:
    #     #     i['driverEffDate'] = format_date(i['driverEffDate'])
    #     #     i['driverExpDate'] = format_date(i['driverExpDate'])
        
        
        
    #     if i['driverBirthDate'] == 'nan':
    #         print(i['driverBirthDate'])
            
    #         i['driverBirthDate'] = '1/1/1900'
            
    #         driverReportPolicy_editOne = driverReportPolicy[driverReportPolicy['DRVFIRSTNAME'] == i['driverFirstName']]
    #         driverReportPolicyFinal = driverReportPolicy_editOne[driverReportPolicy_editOne['DRVLASTNAME'] == i['driverLastName']]
            
    #         print(i['driverFirstName'], i['driverLastName'], driverReportPolicyFinal)
            
    #         if driverReportPolicyFinal['BIRTH_DATE'].values[0] != 'NULL':
    #             print(str(driverReportPolicyFinal['BIRTH_DATE'].values[0]))
            
    #             i['driverBirthDate'] = str(driverReportPolicyFinal['BIRTH_DATE'].values[0])
            
            
            
        
    #     driverFirstNames.append(i['driverFirstName'])
    #     driverLastNames.append(i['driverLastName'])
            
    
    # for index, row in driverReportPolicy.iterrows():
    #     if row['DRVFIRSTNAME'] in driverFirstNames and row['DRVLASTNAME'] in driverLastNames:
            
    #         row=row
    #     else:
    #         print(row['DRVFIRSTNAME'], row['DRVLASTNAME'] )
    #         driverObject = {
    #             "driverFirstName": "null",
    #             "driverMiddleName": "null",
    #             "driverLastName": "null",
    #             "states": "Oregon",
    #             "licenseNumber": "null",
    #             "licenseEffDate": "null",
    #             "licenseExpDate": "null",
    #             "driverEffDate": "null",
    #             "driverExpDate": "null",
    #             "driverBirthDate": "null"
    #         }
            
    #         driverObject['driverFirstName'] = str(row['DRVFIRSTNAME'])
    #         driverObject['driverLastName'] = str(row['DRVLASTNAME'])
    #         driverObject['driverMiddleName'] = str(row['DRIVERMIDDLENAME'])
    #         driverObject['states'] = str(row['DRVLICSTATE'])
    #         driverObject['licenseNumber'] = str(row['DRIVER LICENSE NUMBER'])
    #         driverObject['licenseEffDate'] = str(row['DRVLICEFFDATE'])
    #         driverObject['licenseExpDate'] = str(row['DRVLICEXPDATE'])
    #         driverObject['driverEffDate'] = str(row['DRVEFFDATE'])
    #         driverObject['driverExpDate'] = str(row['DRVEXPDATE'])
    #         driverObject['driverBirthDate'] = str(row['BIRTH_DATE'])
            
    #         policy_json['drivers']['values'].append(driverObject)
        
    
    
    # for i in policy_json['drivers']['values']:
        
        
    
            
    
    # updated_policy = json.dumps(policy_json)
    # collection.update_one({'_id': result['_id']}, {'$set': {'policyJson': updated_policy}})
    
    # updated_policy = json.dumps(policy_json)
    updated_endorsements = json.dumps(endorsements_json)
    # collection.update_one({'_id': result['_id']}, {'$set': {'policyJson': updated_policy}})
    endorsements.update_one({'_id': end_result[0]['_id']}, {'$set': {'endorsementsJson': updated_endorsements}})