import pandas as pd

def flatten_project_data(df):
    project_rows = []

    for _, row in df.iterrows():
        base = {
            "project_id": row.get("project_id"),
            "project_name": row.get("project_name"),
            "status": row.get("status"),
            "client_name": row["client"]["name"],
            "client_industry": row["client"]["industry"],
            "client_city": row["client"]["location"]["city"],
            "client_country": row["client"]["location"]["country"],
            "project_manager": row["team"]["project_manager"],
            "technologies": ", ".join(row["technologies"])
        }

        for member in row["team"]["members"]:
            entry = base.copy()
            entry["member_name"] = member["name"]
            entry["member_role"] = member["role"]
            project_rows.append(entry)

    return pd.DataFrame(project_rows)

def extract_milestones(df):
    milestone_rows = []

    for _, row in df.iterrows():
        for milestone in row["milestones"]:
            milestone_rows.append({
                "project_id": row["project_id"],
                "milestone_name": milestone["name"],
                "milestone_due_date": milestone["due_date"]
            })

    return pd.DataFrame(milestone_rows)
