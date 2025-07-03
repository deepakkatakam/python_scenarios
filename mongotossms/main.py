from src.extract import extract_from_mongodb
from src.transform import transform_projects_data
from src.load import load_to_sql
from src.db import get_sqlserver_engine

def main():
    print("Extracting data from MongoDB...")
    raw_data=extract_from_mongodb()

    print("Transforming data...")
    df=transform_projects_data(raw_data)

    print("Loading into SQL Server...")
    engine=get_sqlserver_engine()
    load_to_sql(df,"customer_projects",engine)

if __name__=="__main__":
    main()