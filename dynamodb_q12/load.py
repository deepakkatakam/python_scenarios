import boto3
from configparser import ConfigParser
from decimal import Decimal

def convert_types(item):
    if isinstance(item,dict):
        return {k: convert_types(v) for k,v in item.items()}
    
    elif isinstance(item,list):
        return [convert_types(v) for v in item]
    elif isinstance(item, float) or isinstance(item,int):
        return Decimal(str(item))
    else:
        return item
    
def load_to_dynamodb(data):
    config=ConfigParser()
    config.read("config.ini")

    region=config.get('dynamodb','region')
    table_name=config.get('dynamodb','table')
    access_key=config.get('dynamodb','aws_access_key_id')
    secret_key=config.get('dynamodb','aws_secret_access_key')
    
    dynamodb=boto3.resource(
        'dynamodb',
        region_name=region,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key)
    table=dynamodb.Table(table_name)

    for item in data:
        item.pop('_id',None)
        item=convert_types(item)
        table.put_item(Item=item)