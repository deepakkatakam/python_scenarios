from src.extract import extract_from_dynamodb
from src.transform import transform_data
from src.load import load_to_sqlserver

if __name__ == "__main__":
    print("🔄 Extracting data from DynamoDB...")
    data = extract_from_dynamodb()
    print(f"✅ Extracted {len(data)} records")

    print("🔍 Sorting and organizing...")
    df = transform_data(data)

    print("📤 Loading into SQL Server...")
    load_to_sqlserver(df)

    print("✅ ETL process completed!")
