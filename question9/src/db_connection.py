import configparser
from sqlalchemy import create_engine
from urllib.parse import quote_plus

def get_mysql_engine():
    config = configparser.ConfigParser()
    config.read("config/config.ini")

    mysql = config["mysql"]
    user = mysql["user"]
    password = quote_plus(mysql["password"])  # Escape special characters like @
    host = mysql["host"]
    port = mysql.get("port", "3306")
    database = mysql["database"]

    connection_string = f"mysql+mysqlconnector://{user}:{password}@{host}:{port}/{database}"
    return create_engine(connection_string)


def get_sqlserver_engine():
    config = configparser.ConfigParser()
    config.read("config/config.ini")

    sqlserver = config["sqlserver"]
    user = sqlserver["username"]
    password = quote_plus(sqlserver["password"])
    server = sqlserver["server"]
    database = sqlserver["database"]
    driver = quote_plus(sqlserver["driver"])

    connection_string = (
        f"mssql+pyodbc://{user}:{password}@{server}/{database}?driver={driver}"
    )
    return create_engine(connection_string)
