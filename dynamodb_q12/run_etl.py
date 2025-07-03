from extract import extract_projects
from load import load_to_dynamodb
if __name__=="__main__":
    print("extracting data from monogodb")
    data=extract_projects()
    print("extracted {len(data)} records")
    print("loading data into dynamodb")
    load_to_dynamodb(data)
    print("load complete")