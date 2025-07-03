import pandas as pd
from configparser import ConfigParser
from pymongo import MongoClient

def extract_projects():
    config=ConfigParser()
    config.read("config.ini")

    mongo_uri=config.get("mongodb","uri")
    db_name=config.get("mongodb","database")
    collection_name=config.get("mongodb","collection")

    client=MongoClient(mongo_uri)
    collection=client[db_name][collection_name]

    data=list(collection.find())
    return data