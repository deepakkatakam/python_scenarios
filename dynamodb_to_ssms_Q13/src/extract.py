import boto3
from configparser import ConfigParser

def extract_from_dynamodb():
    config = ConfigParser()
    config.read('config.ini')

    region = config.get('dynamodb', 'region')
    table_name = config.get('dynamodb', 'table')
    access_key = config.get('dynamodb', 'aws_access_key_id')
    secret_key = config.get('dynamodb', 'aws_secret_access_key')

    dynamodb = boto3.resource(
        'dynamodb',
        region_name=region,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key
    )

    table = dynamodb.Table(table_name)
    response = table.scan()
    data = response.get('Items', [])

    # handle pagination
    while 'LastEvaluatedKey' in response:
        response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
        data.extend(response.get('Items', []))

    return data
