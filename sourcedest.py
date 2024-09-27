from pymongo import MongoClient

# Define the connection strings for the source and destination databases
destination_conn_str = 'mongodb+srv://Chandra99:4JImUai1YehEZskx@cluster0.dnkqjtb.mongodb.net/?retryWrites=true&w=majority'
source_conn_str = 'mongodb+srv://Chandra99:Meteor11@suryasystems.zx4gl.mongodb.net/?retryWrites=true&w=majority'

# Create MongoDB clients for the source and destination databases
source_client = MongoClient(source_conn_str)
destination_client = MongoClient(destination_conn_str)

# Access the source and destination databases and collections
source_db = source_client['test']
destination_db = destination_client['test']
source_collection = source_db['Applications']
destination_collection = destination_db['Applications']

# Fetch all documents from the source collection
documents = list(source_collection.find({}).limit(10))

# Insert documents into the destination collection
destination_collection.insert_many(documents)

# Close the MongoDB clients
source_client.close()
destination_client.close()

print(f'Transferred {len(documents)} documents from source to destination database.')

