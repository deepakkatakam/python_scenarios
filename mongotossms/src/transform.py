import pandas as pd

def transform_projects_data(data):
    rows = []
    for doc in data:
        row = {
            "project_id": doc.get("project_id"),
            "project_name": doc.get("project_name"),
            "client": doc.get("client"),
            "domain": doc.get("domain"),
            "location": doc.get("location"),
            "technologies": ", ".join(doc.get("technologies", [])),
            "project_manager": doc.get("project_manager"),
            "start_date": doc.get("start_date"),
            "end_date": doc.get("end_date"),
            "status": doc.get("status")
        }
        rows.append(row)
    
    return pd.DataFrame(rows)
