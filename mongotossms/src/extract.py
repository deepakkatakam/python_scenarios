import configparser
from pymongo import MongoClient

config=configparser.ConfigParser()
config.read("C:/Users/Deepak/Desktop/python_practice/question11/config.ini")

def extract_from_mongodb():
    uri=config["mongodb"]["uri"]
    db_name=config["mongodb"]["database"]
    collection_name=config["mongodb"]["collection"]

    client=MongoClient(uri)
    collection=client[db_name][collection_name]

    documents=list(collection.find())
    return documents