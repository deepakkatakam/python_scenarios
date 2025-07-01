import configparser
from urllib.parse import quote_plus          # ← add this
from sqlalchemy import create_engine
import pandas as pd

# Read configuration
config = configparser.ConfigParser()
config.read("config.ini")

def get_mysql_engine():
    mysql_cfg = config['mysql']
    # safely quote the password so special chars (@, :, /, etc.) don’t break the URI
    safe_pwd = quote_plus(mysql_cfg['password'])        # ← add this
    conn_str = (
        f"mysql+pymysql://{mysql_cfg['user']}:{safe_pwd}"
        f"@{mysql_cfg['host']}:{mysql_cfg['port']}/{mysql_cfg['database']}"
    )
    return create_engine(conn_str)

# Function to load a DataFrame into MySQL
def load_to_mysql(df: pd.DataFrame, table_name: str):
    engine = get_mysql_engine()
    print(f"📤 Loading table `{table_name}` to MySQL...")
    df.to_sql(name=table_name, con=engine, if_exists='replace', index=False)
    print(f"✅ Loaded `{table_name}` successfully.")
