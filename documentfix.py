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

# Define your filter criteria
filter_criteria = {}

# Query the collection using the filter criteria
results = collection.find()

paymentreport = pd.read_excel("paymentchange2.xlsx")

policy_list = [
  '23CAN00272'
]


# Loop through the results and print them
for result in results:
    policy_json = json.loads(result['policyJson'])
    policyNum = policy_json['policy']['policyNum']
    driverReportPolicy = paymentreport[paymentreport['POLICY NUMBER'] == policyNum]
    
    limits = [   'combinedSectionLimit',  'splitSectionBodyPerPerson',    'splitSectionBodyPerAccidentOptions',    'splitSectionPropertyDamageOptions',    'pIProtectionSingleLimit',    'pIProtectionSplitBodyPerPerson',    'pIProtectionSplitBodyPerAccident',    'pIProtectionSplitPropertyDamage',    'medicalSingleLimit',    'medicalSplitBodyPerPerson',    'medicalSplitBodyPerAccident',    'medicalSplitPropertyDamage',   'underinsuredMotoristSingleLimit',    'underMotoristBodyPerPerson',    'underMotoristBodyPerAccident',    'underMotoristProperty',     'unMotoristBodyPerAccident',    'unMotoristBodyPerPerson',    'unMotoristProperty',    'uninsuredMotoristSingleLimit']

    # if policyNum not in policy_list:
    #     continue 
    
    try:
        for i in policy_json['Uploads']:
            new_uploads = []
            for j in policy_json['Uploads'][i]:
                
                if j[4] == ':':
                    upload = j.replace('http', 'https')
                    new_uploads.append(upload)
                else:
                    new_uploads.append(j)

            policy_json['Uploads'][i] = new_uploads
    except:
        print(policyNum)
            
    
    updated_policy = json.dumps(policy_json)
    collection.update_one({'_id': result['_id']}, {'$set': {'policyJson': updated_policy}})