from pymongo import MongoClient
import pandas as pd

client=MongoClient("mongodb://localhost:27017")
db=client["project_db"]
collection=db["project_details"]

cursor=collection.find()

data=list(cursor)
if data:
    df=pd.DataFrame(data)

    if '_id' in df.columns:
        df.drop(columns=['_id'], inplace=True)

        print(" Extracted DataFrame:")
        print(df.head())

        df.to_csv("extracted_customer_profiles.csv", index=False)

        print("Data saved to 'extracted_customer_profiles.csv'")
else:
    print("No documents found in collection.")