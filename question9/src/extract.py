import pandas as pd
from .db_connection import get_mysql_engine

def extract_customers():
    engine = get_mysql_engine()
    query = "SELECT * FROM customers_1"
    df = pd.read_sql(query, engine)
    return df
