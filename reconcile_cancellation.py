import pandas as pd

reconcile_cancellations = pd.read_excel("PolicyInfoReport.xlsx")

vehicle_policyInfo_mapping = {
      'overallPremium': 'LIAB_PREM',
      'personalInjuryProtectionPremium': 'PIP_PREM',
      'pedPipProtectionPremium': 'PEDPIP_PREM',
      'underinsuredMotoristPremium': 'UIM__PREM',
      'uninsuredMotoristPremium': 'UM__PREM',
      'medicalPaymentsPremium': 'MED_PREM'
      
      
}
premiums_vehicle = ['overallPremium', 'personalInjuryProtectionPremium', 'medicalPaymentsPremium', 'underinsuredMotoristPremium', 'uninsuredMotoristPremium', 'pedPipProtectionPremium']

import json

# Open the text file containing the JSON data
with open('policies.txt', 'r') as file:
    # Read the contents of the file into a string variable
    data_string = file.read()
    
data_string = data_string.replace("'", '"')
data_strings = json.loads(data_string)

print(data_strings)
#data_strings = data_string.strip().split('\n')

data = []
for data_str in data_strings:
    print(data_str)
    data_str = data_str.replace("'", "\"")
    data.append(json.loads(data_str))

# Use the json.loads method to parse the JSON data string into a Python object
# policies = json.loads(data_string)

for policy_one in data:
    print(policy_one, policy_one['policy']['policyNum'])
    reconcile_cancellations = reconcile_cancellations[(reconcile_cancellations['Policy__'].str.strip())==(policy_one['policy']['policyNum'].strip())].reset_index()
    print(reconcile_cancellations, 'before')
    reconcile_cancellations = reconcile_cancellations[reconcile_cancellations['Tran_Code'].isin(['Cancel', 'Cancellation'])]
    
    if len(reconcile_cancellations) > 0:
            print(reconcile_cancellations, 'after')
            policy_one['cancellation']['isCancelled'] = 'Yes'
            
            
            for index, row in reconcile_cancellations.iterrows():
                policy_one['cancellation']['cancellationDate'] = row['Endt_Eff']
                for j in policy_one['vehicles']['values']:
                        
                        if row['Vin'] == j['vin']:
                            
                            for premium in premiums_vehicle:
                                    
                                    if j[premium] + row[vehicle_policyInfo_mapping[premium]] < 0:
                                        j[premium] = 0
                                    else:
                                        j[premium] += row[vehicle_policyInfo_mapping[premium]]
    
      
      