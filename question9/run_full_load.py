from src.extract import extract_customers
from src.db_connection import get_sqlserver_engine
from src.load import full_load

if __name__ == "__main__":
    df = extract_customers()
    engine = get_sqlserver_engine()
    full_load(df, engine)
    print("Full Load Completed")
