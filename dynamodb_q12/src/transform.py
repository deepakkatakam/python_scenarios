import pandas as pd

def flatten_projects(project):
    flat={
         "project_id":project.get("project_id"),
         "project_name":project.get("project_name"),
         "client_name":project.get("client",{}).get("name"),
         "industry":project.get("client",{}).get("industry"),
         
    }