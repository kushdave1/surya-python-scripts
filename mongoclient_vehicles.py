from pymongo import MongoClient
import json
import pandas as pd
import math
import datetime
import copy

# Connect to the MongoDB instance
client = MongoClient('mongodb+srv://Chandra99:Meteor11@suryasystems.zx4gl.mongodb.net/?retryWrites=true&w=majority')

# Get the database you want to work with
db = client['test']

# Get the collection you want to filter
collection = db['Policies']

endorsements = db['Endorsements']

# Define your filter criteria

# Query the collection using the filter criteria
results = collection.find()

driverreport = pd.read_excel("Final_driver_report_missingcheck.xlsx")


# Loop through the results and print them
for result in results:
    policy_json = json.loads(result['policyJson'])
    policyNum = policy_json['policy']['policyNum']



    # if policyNum != '19AZT00011':
    #     continue
    filter_criteria = {'policyNum': policyNum}
    end_result = endorsements.find(filter_criteria)
    
    # if policyNum != '22NJL00093':
    #     continue
    
    try:
        endorsements_json = json.loads(end_result[0]['endorsementsJson'])
    except:
        continue
    
    # if len(end_result) > 0:

    #     endorsements_json = json.loads(end_result[0]['endorsementsJson'])
    # else:
    #     continue
    
    
            
    
        
    
    driverReportPolicy = driverreport[driverreport['DRVPOLICYNO'] == policyNum]
    
    limits = [   'combinedSectionLimit',  'splitSectionBodyPerPerson',    'splitSectionBodyPerAccidentOptions',    'splitSectionPropertyDamageOptions',    'pIProtectionSingleLimit',    'pIProtectionSplitBodyPerPerson',    'pIProtectionSplitBodyPerAccident',    'pIProtectionSplitPropertyDamage',    'medicalSingleLimit',    'medicalSplitBodyPerPerson',    'medicalSplitBodyPerAccident',    'medicalSplitPropertyDamage',   'underinsuredMotoristSingleLimit',    'underMotoristBodyPerPerson',    'underMotoristBodyPerAccident',    'underMotoristProperty',     'unMotoristBodyPerAccident',    'unMotoristBodyPerPerson',    'unMotoristProperty',    'uninsuredMotoristSingleLimit']
    premiums = ['overallPremium',
        'personalInjuryProtectionPremium',
        'pedPipProtectionPremium',
        'medicalPaymentsPremium',
        'underinsuredMotoristPremium',
        'uninsuredMotoristPremium']
    # for i in policy_json['drivers']['values']:
    #     if i['driverFirstName'].isin(driverReportPolicy['DRVFIRSTNAME']) and i['driverLastName'].isin(driverReportPolicy['DRVLASTNAME']):
    #         print(i)
    def has_letter(str):
        return any(c.isalpha() for c in str)
    
    # vehicles = policy_json['vehicles']['values']
    
    # for index, vehicle in enumerate(vehicles):
    #     if vehicle['baseExpDate'] != policy_json['policy']['expirationDate'] and vehicle['baseExpDate'] != None:
    #         index_string = 'vehicles.values[%s].baseExpDate' % index
    #         # endorsements_json[]
            
    #         if index_string not in endorsements_json:
    #             print(policyNum, 'lsl')
    #             endorsements_json[index_string] = { "values" : [{
    #                 'oldValue': policy_json['policy']['expirationDate'],
    #                 'newValue': vehicle['baseExpDate'],
    #                 'time': datetime.datetime.strptime(vehicle['baseExpDate'], '%m/%d/%Y').strftime('%Y-%m-%dT%H:%M:%S.%f'),
    #                 'effDate': vehicle['baseExpDate']
    #             }]
    #             }
    
    
    vehicles = policy_json['vehicles']['values']
    
    for index, vehicle in enumerate(vehicles):
        
        print(vehicle)
        
        if vehicle['baseEffDate'] != policy_json['policy']['effectiveDate'] and vehicle['baseEffDate'] != None:
            end_vehicles = [vehicle]
        
        
            
        
            for dr in vehicles:
                if dr not in end_vehicles and vehicle['baseEffDate'] == dr['baseEffDate'] and vehicle['vin'] != dr['vin']:
             
                    end_vehicles.append(dr)

            index_string = 'vehicles.values'
            # endorsements_json[]
            indicator = False
            
            if index_string in endorsements_json:

            
                for i in endorsements_json[index_string]['values']:
                    old_value = i['oldValue']
                    new_value = i['newValue']
                    
                    
                    if old_value is not None and new_value is not None:
                        new_values_only = [item for item in new_value if item not in old_value]
                        
                        if new_values_only is not None:
        
                            for j in new_values_only:
                                if vehicle == j:
                                    
                        
                                    indicator = True

                    
             
                if indicator == False:
                    

                    try:
                        older_vehicles = [item for item in vehicles if datetime.datetime.strptime(item['baseEffDate'].replace('//','/'), '%m/%d/%Y') < datetime.datetime.strptime(vehicle['baseEffDate'].replace('//','/'), '%m/%d/%Y')]
                    except:
     
                        continue
                    
                    
                    endorsements_json[index_string]['values'].append({
                        'oldValue': older_vehicles,
                        'newValue': older_vehicles+end_vehicles,
                        'time': datetime.datetime.strptime(vehicle['baseEffDate'].replace('//','/').strip(), '%m/%d/%Y').replace(hour=6, minute=0, second=0, microsecond=0).strftime('%Y-%m-%dT%H:%M:%S.%f'),
                        'effDate': vehicle['baseEffDate']
                    })

            else:
                
                print(vehicle['baseEffDate'])
                older_vehicles = [item for item in vehicles if datetime.datetime.strptime(item['baseEffDate'].replace('//','/'), '%m/%d/%Y') < datetime.datetime.strptime(vehicle['baseEffDate'].replace('//','/'), '%m/%d/%Y')]
                
                
                
                endorsements_json[index_string] = { 'values' : [{
                    'oldValue': older_vehicles,
                    'newValue': older_vehicles+end_vehicles,
                    'time': datetime.datetime.strptime(vehicle['baseEffDate'].strip(), '%m/%d/%Y').replace(hour=6, minute=0, second=0, microsecond=0).strftime('%Y-%m-%dT%H:%M:%S.%f'),
                    'effDate': vehicle['baseEffDate']
                }]}

                
            # if index_string not in endorsements_json and '-' not in driver['driverEffDate'].strip():
            #     endorsements_json[index_string] = { "values" : [{
            #         'oldValue': policy_json['policy']['expirationDate'],s
            #         'newValue': driver['driverExpDate'],
            #         'time': datetime.datetime.strptime(driver['driverExpDate'].strip(), '%m/%d/%Y').strftime('%Y-%m-%dT%H:%M:%S.%f'),
            #         'effDate': driver['driverExpDate']
            #     }]
            #     }
    try:               
        for end in endorsements_json['vehicles.values']['values']:
            if len(end['oldValue']) == len(end['newValue']):
                endorsements_json['vehicles.values']['values'].remove(end)
    except:
        continue
    # for index, end in enumerate(endorsements_json['drivers.values']['values']):     
    #     result_list = [dct for dct in end['newValue'] if dct not in end['oldValue']]
        
    #     for index2, end2 in enumerate(endorsements_json['drivers.values']['values']):
    #         result_list_new = [dct for dct in end2['newValue'] if dct not in end2['oldValue']]
            
    #         if result_list['driverFirstName'] == result_list_new['driverFirstName'] and index != index2:
    #             print(result_list)
        
    
    
        
        
    
    # if policy_json['coverage']['splitSectionBodyPerPerson'] == 'nan' or policy_json['coverage']['splitSectionBodyPerAccidentOptions'] == 'nan':
    #     continue
    
    
    # if int(policy_json['coverage']['splitSectionBodyPerPerson'].replace(",", "")) >  int(policy_json['coverage']['splitSectionBodyPerAccidentOptions'].replace(",", "")):
    #     person = copy.copy(policy_json['coverage']['splitSectionBodyPerPerson'])
    #     accident = copy.copy(policy_json['coverage']['splitSectionBodyPerAccidentOptions'])
    #     policy_json['coverage']['splitSectionBodyPerAccidentOptions'] = person
    #     policy_json['coverage']['splitSectionBodyPerPerson'] = accident
    #     number_string = policy_json['coverage'][i]
    
    
    #     if number_string != 'nan':
            
    #         if has_letter(str(number_string))
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
        
    
    # vehicles = policy_json['vehicles']['values']
    
    # driverFirstNames = []
    # driverLastNames = []
    
    # def format_date(date_str):
    #     # Parse the input date string as a datetime object
        
    #     if ":" in date_str:
    #         date_obj = datetime.datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S')
    #     else: 
    #         date_obj = datetime.datetime.strptime(date_str, '%Y-%m-%d')
        
    #     # Format the date object as a string with the desired format
    #     formatted_date = date_obj.strftime('%m/%d/%Y')
        
    #     return formatted_date
    
    # for i in vehicles:
        
    #     if len(i['baseEffDate'].split('/')[2]) < 4:
    #         print(i['baseEffDate'])
    #         effDate = datetime.datetime.strptime(i['baseEffDate'], '%m/%d/%y')
    #         i['baseEffDate'] = datetime.datetime.strftime(effDate, '%m/%d/%Y')
        
    #     if len(i['baseExpDate'].split('/')[2]) < 4:
    #         print(i['baseExpDate'])
    #         expDate = datetime.datetime.strptime(i['baseExpDate'], '%m/%d/%y')
    #         i['baseExpDate'] = datetime.datetime.strftime(expDate, '%m/%d/%Y')
        
        
    #     effDate = datetime.datetime.strptime(i['baseEffDate'], '%m/%d/%Y')
    #     expDate = datetime.datetime.strptime(i['baseExpDate'], '%m/%d/%Y')
        
    #     delta = expDate - effDate
    #     num_days = delta.days
        
    #     for j in premiums:
    #         totalPremium = policy_json['coverage'][j]
    #         if totalPremium == '':
    #             totalPremium = 0
                
    #         if num_days > 365:
    #             num_days = 365
    #         i[j] = "{:.2f}".format(float(totalPremium)*(num_days/365))
        
        
        
        # if '-' in i['licenseEffDate']:
            
        #     i['licenseEffDate'] = format_date(i['licenseEffDate'])
        #     i['licenseExpDate'] = format_date(i['licenseExpDate'])
        # if '-' in i['driverBirthDate']:
        #     i['driverBirthDate'] = format_date(i['driverBirthDate'])
        
        
        # if i['driverBirthDate'] == 'nan':
        #     print(i['driverBirthDate'])
            
        #     i['driverBirthDate'] = '1/1/1900'
            
        #     driverReportPolicy_editOne = driverReportPolicy[driverReportPolicy['DRVFIRSTNAME'] == i['driverFirstName']]
        #     driverReportPolicyFinal = driverReportPolicy_editOne[driverReportPolicy_editOne['DRVLASTNAME'] == i['driverLastName']]
            
        #     print(i['driverFirstName'], i['driverLastName'], driverReportPolicyFinal)
            
        #     if driverReportPolicyFinal['BIRTH_DATE'].values[0] != 'NULL':
        #         print(str(driverReportPolicyFinal['BIRTH_DATE'].values[0]))
            
        #         i['driverBirthDate'] = str(driverReportPolicyFinal['BIRTH_DATE'].values[0])
            
            
            
        
    #     driverFirstNames.append(i['driverFirstName'])
    #     driverLastNames.append(i['driverLastName'])
            
    
    # for index, row in driverReportPolicy.iterrows():
    #     if row['DRVFIRSTNAME'] in driverFirstNames and row['DRVLASTNAME'] in driverLastNames:
            
    #         row=row
    #     else:
    #         print(row['DRVFIRSTNAME'], row['DRVLASTNAME'] )
            # driverObject = {
            #     "driverFirstName": "null",
            #     "driverMiddleName": "null",
            #     "driverLastName": "null",
            #     "states": "Oregon",
            #     "licenseNumber": "null",
            #     "licenseEffDate": "null",
            #     "licenseExpDate": "null",
            #     "driverEffDate": "null",
            #     "driverExpDate": "null",
            #     "driverBirthDate": "null"
            # }
            
            # driverObject['driverFirstName'] = str(row['DRVFIRSTNAME'])
            # driverObject['driverLastName'] = str(row['DRVLASTNAME'])
            # driverObject['driverMiddleName'] = str(row['DRIVERMIDDLENAME'])
            # driverObject['states'] = str(row['DRVLICSTATE'])
            # driverObject['licenseNumber'] = str(row['DRIVER LICENSE NUMBER'])
            # driverObject['licenseEffDate'] = str(row['DRVLICEFFDATE'])
            # driverObject['licenseExpDate'] = str(row['DRVLICEXPDATE'])
            # driverObject['driverEffDate'] = str(row['DRVEFFDATE'])
            # driverObject['driverExpDate'] = str(row['DRVEXPDATE'])
            # driverObject['driverBirthDate'] = str(row['BIRTH_DATE'])
            
            # policy_json['drivers']['values'].append(driverObject)
        
    
    
    # for i in policy_json['drivers']['values']:
        
        
    
            
    
    updated_policy = json.dumps(policy_json)
    updated_endorsements = json.dumps(endorsements_json)
    # collection.update_one({'_id': result['_id']}, {'$set': {'policyJson': updated_policy}})
    endorsements.update_one({'_id': end_result[0]['_id']}, {'$set': {'endorsementsJson': updated_endorsements}})