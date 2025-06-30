
from src.extract import extract_customers
from src.db_connection import get_sqlserver_engine
from src.utils import get_existing_customers
from src.transform import transform_incremental
from src.load import incremental_load

if __name__ == "__main__":
    df_new = extract_customers()
    sql_engine = get_sqlserver_engine()
    existing = get_existing_customers(sql_engine)

    new_records, updated_records = transform_incremental(df_new, existing)
    if new_records or updated_records:
        incremental_load(new_records, updated_records, sql_engine)
        print("✅ Incremental Load Completed")
    else:
        print("ℹ️ No changes detected.")
