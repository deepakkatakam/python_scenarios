import json
from pymongo import MongoClient
import os

client=MongoClient("mongodb://localhost:27017")
db=client["project_db"]
collection=db["unstructured_details"]

file_path=os.path.join("..","data","Doc_unstructured_1.txt")
with open(file_path,"r") as file:
    data=json.load(file)

if isinstance(data,list):
    result=collection.insert_many(data)
    print(f"{len(result.inserted_ids)} records uploaded successfully!")
else:
    print("Expected a list of project dictionaries")