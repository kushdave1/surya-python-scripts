from pymongo import MongoClient
import json
import pandas as pd
import math
import datetime
import copy
import json
import hashlib
from datetime import datetime


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


codes = [
  
  "22NJN00255"
]

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
    
    if policyNum not in codes:
        continue
    

    
    
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
    
    endorsement_keys = list(endorsements_json.keys())

    # for index, vehicle in enumerate(vehicles):
        

    #     if vehicle['baseEffDate'] != policy_json['policy']['effectiveDate']:
    #         print(vehicle['vin'])

    #         if 'vehicles.values' in endorsement_keys:
    #             for index, end in enumerate(endorsements_json['vehicles.values']['values']):
    #                 for index2, end2 in enumerate(endorsements_json['vehicles.values']['values']):
                        
    #                     if end['oldValue'] == end2['oldValue'] and end['newValue'] == end2['newValue'] and index != index2:
    #                         endorsements_json['vehicles.values']['values'].remove(end2)
                        
    inner_index = 0
    if 'vehicles.values' in endorsement_keys:
        while inner_index < len(endorsements_json['vehicles.values']['values']):
            end = endorsements_json['vehicles.values']['values'][inner_index]
            end_copy = copy.copy(end)
            if 'time' in list(end_copy.keys()):
                del end_copy['time']
                
        
            
            for index2, end2 in enumerate(endorsements_json['vehicles.values']['values']):
                end2_copy = copy.copy(end2)
                if 'time' in list(end2_copy.keys()):
                    del end2_copy['time']
                
        
                if len(end_copy['oldValue']) == len(end2_copy['oldValue']) and len(end_copy['newValue']) == len(end2_copy['newValue']) and datetime.strptime(end_copy['effDate'], '%m/%d/%Y') == datetime.strptime(end2_copy['effDate'], '%m/%d/%Y') and inner_index != index2:
                    print('hi')
                    endorsements_json['vehicles.values']['values'].remove(end2)
                    inner_index -= 1  # Decrement the inner index to account for the removed object
            
            inner_index += 1  # Incre                    
                #             if new_values_only1[0]['vin'] == new_values_only2[0]['vin'] and index != index2:
                #                 print(new_values_only1)
                #                 endorsements_json['vehicles.values']['values'].remove(end2)
                            
                        
                             
        
        # string_index = "vehicles.values[%s].vin" % index
        
        # exp_index = "vehicles.values[%s].baseExpDate" % index
        
        
        # if vehicle['baseEffDate'] != policy_json['policy']['effectiveDate']:
            
        #     for i in endorsement_keys:
        #         try:
        #             if endorsements_json[i]['values'][0]['newValue'] == vehicle['vin']:
                    
                    
        #                 vehicle_end = endorsements_json['vehicles.values']['values']
        #                 for j in vehicle_end:
        #                     old_value = j['oldValue']
        #                     new_value = j['newValue']
                            
                            
        #                     if old_value is not None and new_value is not None:
        #                         new_values_only = [item for item in new_value if item not in old_value]
                            
        #                         for k in new_values_only:
        #                             if k['vin'] == vehicle['vin'] and len(new_values_only) > 1:
        #                                 new_value.remove(k)
        #                             elif k['vin'] == vehicle['vin'] and len(new_values_only) == 1:
        #                                 vehicle_end.remove(j)
                            
        #         except:
        #             continue
        
        

            

    
    # updated_policy = json.dumps(policy_json)
    # collection.update_one({'_id': result['_id']}, {'$set': {'policyJson': updated_policy}})
    
    # updated_policy = json.dumps(policy_json)
    updated_endorsements = json.dumps(endorsements_json)
    # collection.update_one({'_id': result['_id']}, {'$set': {'policyJson': updated_policy}})
    endorsements.update_one({'_id': end_result[0]['_id']}, {'$set': {'endorsementsJson': updated_endorsements}})