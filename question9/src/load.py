from datetime import datetime
from sqlalchemy import text

def full_load(df, sql_engine):
    now = datetime.now()
    df['start_date'] = now
    df['end_date'] = None
    df['is_current'] = True
    df['loaded_at'] = now
    df.to_sql("customers_scd2", sql_engine, if_exists='replace', index=False)

def incremental_load(new_records, updated_records, sql_engine):
    conn = sql_engine.connect()
    tx = conn.begin()
    try:
        now = datetime.now()

        # Expire old versions
        for old, _ in updated_records:
            conn.execute(text("""
                UPDATE customers_scd2
                SET end_date = :now, is_current = 0
                WHERE customer_id = :cid AND is_current = 1
            """), {"now": now, "cid": old.customer_id})

        # Insert updated records
        for _, new in updated_records:
            conn.execute(text("""
                INSERT INTO customers_scd2 (
                    customer_id, name, address, phone, email, start_date, end_date, is_current, loaded_at
                ) VALUES (
                    :cid, :name, :address, :phone, :email, :start_date, NULL, 1, :loaded_at
                )
            """), {
                "cid": new['customer_id'],
                "name": new['name'],
                "address": new['address'],
                "phone": new['phone'],
                "email": new['email'],
                "start_date": now,
                "loaded_at": now
            })

        # Insert new records
        for new in new_records:
            conn.execute(text("""
                INSERT INTO customers_scd2 (
                    customer_id, name, address, phone, email, start_date, end_date, is_current, loaded_at
                ) VALUES (
                    :cid, :name, :address, :phone, :email, :start_date, NULL, 1, :loaded_at
                )
            """), {
                "cid": new['customer_id'],
                "name": new['name'],
                "address": new['address'],
                "phone": new['phone'],
                "email": new['email'],
                "start_date": now,
                "loaded_at": now
            })

        tx.commit()
    except Exception as e:
        tx.rollback()
        raise e
    finally:
        conn.close()
