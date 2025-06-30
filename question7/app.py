from src.extract import extract_data
from src.load import load_analysis_data
from src.transform import transform_data
def main():
    print("Starting ETL....")
    df=extract_data()
    if df is not None:
        print(f" extracted {len(df)} rows from SQL Server.")
        df_transformed, summary,total = transform_data(df)
        load_analysis_data(df_transformed)
        print("\n Sales Summary:\n", summary.to_string(index=False))
        print("Total = ",total)
    else:
        print("No data extracted")

if __name__=="__main__":
    main()