from sqlalchemy import text

def get_existing_customers(sql_engine):
    query = """
        SELECT customer_id, name, address, phone, email, start_date, end_date, is_current, loaded_at
        FROM customers_scd2
        WHERE is_current = 1
    """
    with sql_engine.connect() as conn:
        result = conn.execute(text(query))
        return result.fetchall()

def record_changed(old_row, new_row):
    return (
        old_row.name != new_row['name'] or
        old_row.address != new_row['address'] or
        old_row.phone != new_row['phone'] or
        old_row.email != new_row['email']
    )
