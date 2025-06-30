from datetime import datetime
from .utils import record_changed

def transform_incremental(df_new, df_existing):
    new_records = []
    updated_records = []

    existing_map = {(row.customer_id): row for row in df_existing}

    for _, row in df_new.iterrows():
        cid = row['customer_id']
        if cid not in existing_map:
            new_records.append(row)
        else:
            old = existing_map[cid]
            if record_changed(old, row):
                updated_records.append((old, row))

    return new_records, updated_records
