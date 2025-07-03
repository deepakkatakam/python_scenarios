import configparser
from sqlalchemy import create_engine

config=configparser.ConfigParser()
config.read("config.ini")

def get_sqlserver_engine():
    user=config["sqlserver"]["username"]
    password=config["sqlserver"]["password"]
    server=config["sqlserver"]["server"]
    database=config["sqlserver"]["database"]
    driver=config["sqlserver"]["driver"]

    connection_string = (
        f"mssql+pyodbc://{user}:{password}@{server}/{database}?driver={driver.replace(' ', '+')}"
    )
    return create_engine(connection_string)