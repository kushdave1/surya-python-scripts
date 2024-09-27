from pymongo import MongoClient
import json
import pandas as pd

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

driverreport = pd.read_excel("NaicReport.xlsx")

final_report = pd.DataFrame()


# Loop through the results and print them
for result in results:
    policy_json = json.loads(result['policyJson'])
    policyNum = policy_json['policy']['policyNum']
    
    driverReportPolicy = driverreport[driverreport['Policy__'] == policyNum]
    
    # if policyNum != '22PAT00063':
    #     continue
    
    for index, row in driverReportPolicy.iterrows():
        try:
            if int(float(row['PIPLMT'].replace(",", ""))) > 0:
            
                if row['Tran_Code'] == 'Policy':
                
                
                    policy_json['coverage']['personalInjuryProtectionPremium'] = row['PIP_PREM']
                
                for i in policy_json['vehicles']['values']:
                    if row['Vin'] == i['vin']:
                        i['personalInjuryProtectionPremium'] = row['PIP_PREM']
        except:
            print('hi')        
            
    
    # for i in policy_json['drivers']['values']:
    #     if i['driverFirstName'].isin(driverReportPolicy['DRVFIRSTNAME']) and i['driverLastName'].isin(driverReportPolicy['DRVLASTNAME']):
    #         print(i)
    
    # vehicles = policy_json['vehicles']['values']
    
    # for i in vehicles:
         
        
    
            
    
    updated_policy = json.dumps(policy_json)
    collection.update_one({'_id': result['_id']}, {'$set': {'policyJson': updated_policy}})