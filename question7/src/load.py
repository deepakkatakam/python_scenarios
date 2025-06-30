import pandas as pd
from sqlalchemy import create_engine
from urllib.parse import quote_plus
from src.db_config import get_db_config

def load_analysis_data(df):
    cfg = get_db_config("mysql")

    try:
        # Properly encode password for URI
        password_encoded = quote_plus(cfg['password'])

        # Create SQLAlchemy engine for MySQL
        engine = create_engine(
            f"mysql+mysqlconnector://{cfg['user']}:{password_encoded}@{cfg['host']}:{cfg['port']}/{cfg['database']}"
        )

        # Load data to 'orders' table
        df.to_sql(name='orders', con=engine, if_exists='replace', index=False)
        print("Data successfully loaded into MySQL (table: orders) using SQLAlchemy.")

    except Exception as e:
        print(" MySQL SQLAlchemy error:", e)
