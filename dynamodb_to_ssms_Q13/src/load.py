import pyodbc
from configparser import ConfigParser

def load_to_sqlserver(df):
    config = ConfigParser()
    config.read('config.ini')

    conn_str = (
        f"DRIVER={config.get('sqlserver', 'driver')};"
        f"SERVER={config.get('sqlserver', 'server')};"
        f"DATABASE={config.get('sqlserver', 'database')};"
        f"UID={config.get('sqlserver', 'username')};"
        f"PWD={config.get('sqlserver', 'password')}"
    )
    
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()

    # Ensure table exists
    cursor.execute("""
    IF OBJECT_ID('dbo.unstructured_details', 'U') IS NULL
    CREATE TABLE dbo.unstructured_details (
        project_id VARCHAR(50),
        project_name VARCHAR(255),
        status VARCHAR(100),
        client_name VARCHAR(100),
        team_manager VARCHAR(100)
        -- Add more columns as needed
    )
    """)
    conn.commit()

    for _, row in df.iterrows():
        cursor.execute("""
            INSERT INTO dbo.unstructured_details (project_id, project_name, status, client_name, team_manager)
            VALUES (?, ?, ?, ?, ?)
        """, row.get('project_id'), row.get('project_name'), row.get('status'),
             row.get('client', {}).get('name'), row.get('team', {}).get('project_manager'))

    conn.commit()
    cursor.close()
    conn.close()
