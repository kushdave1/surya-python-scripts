import pymongo

# MongoDB connection setup
def connect_to_mongodb():
    try:
        connection_string = "mongodb+srv://Chandra99:4JImUai1YehEZskx@cluster0.dnkqjtb.mongodb.net/?retryWrites=true&w=majority"
        client = pymongo.MongoClient(connection_string)
        db = client.test
        collection = db.Applications
        return collection
    except Exception as e:
        print(f"Error connecting to MongoDB: {str(e)}")
        return None

# Function to fetch all data from the collection
def fetch_all_data():
    try:
        collection = connect_to_mongodb()
        if collection is None:
            print("collection is none")
            return []  # Return an empty list if there's an error in connecting to MongoDB

        # Use the find method without a query to retrieve all documents
        documents = collection.find()
        print("documents", documents)
        return list(documents)

    except Exception as e:
        print(f"Error fetching data: {str(e)}")
        return []

# Fetch all data from the collection
all_data = fetch_all_data()
print("all_data", all_data)
if all_data:
    for document in all_data:
        print(document)
    print(f"Total documents fetched: {len(all_data)}")
else:
    print("No data was fetched.")