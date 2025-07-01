import json
from pymongo import MongoClient
import os

# Connect to MongoDB at localhost:27017
client = MongoClient("mongodb://localhost:27017")
db = client["project_db"]
collection = db["project_details"]

# Read JSON data from file
file_path = os.path.join("..", "data", "project.txt")
with open(file_path, "r") as file:
    data = json.load(file)

# Insert data
if isinstance(data, list):
    result = collection.insert_many(data)
    print(f"{len(result.inserted_ids)} records uploaded successfully!")
else:
    print("Expected a list of project dictionaries.")
