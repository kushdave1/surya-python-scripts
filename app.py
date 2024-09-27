from pymongo import MongoClient
import json
import pandas as pd

# Connect to the MongoDB instance
client = MongoClient('mongodb+srv://Chandra99:Meteor11@suryasystems.zx4gl.mongodb.net/?retryWrites=true&w=majority')

# Get the database you want to work with
db = client['test']

# Get the collection you want to filter
collection = db['Applications']

# Define your filter criteria
filter_criteria = {}

# Query the collection using the filter criteria
results = collection.find()

driverreport = pd.read_excel("NaicReport.xlsx")

final_report = pd.DataFrame()

new_field = {'Decision': "Undefined"}


update = {'$set': new_field}


collection.update_many({}, update)
# Loop through the results and print them