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
stateCodes = {
    "AL": "Alabama",
    "AK": "Alaska",
    "AS": "American Samoa",
    "AZ": "Arizona",
    "AR": "Arkansas",
    "CA": "California",
    "CO": "Colorado",
    "CT": "Connecticut",
    "DE": "Delaware",
    "DC": "District Of Columbia",
    "FM": "Federated States Of Micronesia",
    "FL": "Florida",
    "GA": "Georgia",
    "GU": "Guam",
    "HI": "Hawaii",
    "ID": "Idaho",
    "IL": "Illinois",
    "IN": "Indiana",
    "IA": "Iowa",
    "KS": "Kansas",
    "KY": "Kentucky",
    "LA": "Louisiana",
    "ME": "Maine",
    "MH": "Marshall Islands",
    "MD": "Maryland",
    "MA": "Massachusetts",
    "MI": "Michigan",
    "MN": "Minnesota",
    "MS": "Mississippi",
    "MO": "Missouri",
    "MT": "Montana",
    "NE": "Nebraska",
    "NV": "Nevada",
    "NH": "New Hampshire",
    "NJ": "New Jersey",
    "NM": "New Mexico",
    "NY": "New York",
    "NC": "North Carolina",
    "ND": "North Dakota",
    "MP": "Northern Mariana Islands",
    "OH": "Ohio",
    "OK": "Oklahoma",
    "OR": "Oregon",
    "PW": "Palau",
    "PA": "Pennsylvania",
    "PR": "Puerto Rico",
    "RI": "Rhode Island",
    "SC": "South Carolina",
    "SD": "South Dakota",
    "TN": "Tennessee",
    "TX": "Texas",
    "UT": "Utah",
    "VT": "Vermont",
    "VI": "Virgin Islands",
    "VA": "Virginia",
    "WA": "Washington",
    "WV": "West Virginia",
    "WI": "Wisconsin",
    "WY": "Wyoming",
    '': "None"
    }

# Define your filter criteria
premiums = ['overallPremium',
        'personalInjuryProtectionPremium',
        'pedPipProtectionPremium',
        'medicalPaymentsPremium',
        'underinsuredMotoristPremium',
        'uninsuredMotoristPremium']
filter_criteria = {}

# Query the collection using the filter criteria
results = collection.find()


for result in results:
    
    policy_json = json.loads(result['policyJson'])
    policyNum = policy_json['policy']['policyNum']
    policyState = policyNum[2:4]
    
    vehicles = policy_json['vehicles']['values']

    for i in premiums:
        if policy_json['coverage'][i] == 'null':
            policy_json['coverage'][i] = 0
    for index1, vehicle1 in enumerate(vehicles):
        for i in premiums:
            print(i)
            if vehicle1[i] == 'null':
                vehicle1[i] = 0
        # if vehicle1['state'] == 'Nebraska' or vehicle1['state'] == 'None':
        #     vehicle1['state'] = stateCodes[policyState]
        #     vehicle1['garageState'] = stateCodes[policyState]

    updated_policy = json.dumps(policy_json)
    collection.update_one({'_id': result['_id']}, {'$set': {'policyJson': updated_policy}})