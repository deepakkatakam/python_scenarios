def load_to_sql(df,table_name,engine):
    df.to_sql(name=table_name,con=engine,if_exists="replace",index=False)
    print(f"Loaded {len(df)} rows into table: {table_name}")