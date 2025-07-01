from src.extract import extract_projects
from src.transform import flatten_project_data, extract_milestones
from src.load import load_to_mysql

def main():
    print("🔄 Extracting data from MongoDB...")
    raw_df = extract_projects()

    print("🔄 Transforming project structure...")
    projects_df = flatten_project_data(raw_df)

    print("🔄 Transforming milestones...")
    milestones_df = extract_milestones(raw_df)

    print("📤 Loading structured data into MySQL...")
    load_to_mysql(projects_df, "projects_structured")
    load_to_mysql(milestones_df, "project_milestones")

    print("✅ ETL completed successfully!")

if __name__ == "__main__":
    main()
