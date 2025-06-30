import configparser

def get_db_config(section):
    config=configparser.ConfigParser()
    config.read(config.read('C:/Users/Deepak/Desktop/python_practice/question7/config.ini'))
    print("Available sections:", config.sections())
    return config[section]