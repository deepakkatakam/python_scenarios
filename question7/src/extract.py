import pandas as pd
from sqlalchemy import create_engine
from urllib.parse import quote_plus
from src.db_config import get_db_config

def extract_data():
    cfg = get_db_config("sqlserver")

    # Build SQLAlchemy connection string
    driver = cfg['driver']
    quoted_conn = quote_plus(
        f"DRIVER={driver};"
        f"SERVER={cfg['server']};"
        f"DATABASE={cfg['database']};"
        f"UID={cfg['username']};"
        f"PWD={cfg['password']};"
        "TrustServerCertificate=yes;"
    )

    # Create SQLAlchemy engine
    connection_string = f"mssql+pyodbc:///?odbc_connect={quoted_conn}"
    
    try:
        engine = create_engine(connection_string)
        df = pd.read_sql("SELECT * FROM orders", engine)
        print("Data extracted using SQLAlchemy.")
        return df
    except Exception as e:
        print("SQL Server SQLAlchemy error:", e)
        return None
