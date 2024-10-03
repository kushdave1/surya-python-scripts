from pymongo import MongoClient
import json
import pandas as pd
import math
import datetime
import logging

logging.basicConfig(filename='policy_updates.log', level=logging.INFO,
                    format='%(asctime)s:%(levelname)s:%(message)s')

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


# Loop through the results and print them
for result in results:
    policy_json = json.loads(result['policyJson'])
    policyNum = policy_json['policy']['policyNum']
    driverReportPolicy = paymentreport[paymentreport['POLICY NUMBER'] == policyNum]

    try:
        policy_json = json.loads(result['policyJson'])
        policyNum = policy_json['policy']['policyNum']

        # Ensure 'Uploads' exists in the JSON
        if 'Uploads' in policy_json:
            # Iterate through each upload entry
            for i in policy_json['Uploads']:
                new_uploads = []
                for j in policy_json['Uploads'][i]:
                    # Replace 'http' with 'https'
                    if j.startswith("http://"):
                        upload = j.replace("http://", "https://")
                        new_uploads.append(upload)
                    else:
                        new_uploads.append(j)

                # Update the policy's uploads
                policy_json['Uploads'][i] = new_uploads

        # Convert the updated policy back to JSON
        updated_policy = json.dumps(policy_json)
        # Update the document in MongoDB
        collection.update_one({'_id': result['_id']}, {'$set': {'policyJson': updated_policy}})
        
        logging.info(f"Updated policy {policyNum} with new uploads.")

    except json.JSONDecodeError as e:
        logging.error(f"JSON decode error for policy {policyNum}: {e}")
    
    except Exception as e:
        logging.error(f"Error updating policy {policyNum}: {e}")
