def transform_data(df):
    df_sorted = df.sort_values(by='order_date', ascending=False)
    summary=df_sorted.groupby('product_category')['order_amount'].sum().reset_index()
    summary.columns=['product_category','total_sales']
    total=df_sorted['order_amount'].sum()
    print(" data transformed.")
    return df_sorted,summary,total