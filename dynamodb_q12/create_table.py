import boto3
from configparser import ConfigParser

def create_dynamodb_table():
    config=ConfigParser()
    config.read('config.ini')

    region=config.get('dynamodb','region')
    table_name = config.get('dynamodb', 'table')
    access_key = config.get('dynamodb', 'aws_access_key_id')
    secret_key = config.get('dynamodb', 'aws_secret_access_key')

    dynamodb=boto3.client(
        'dynamodb',
        region_name=region,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key
    )

    existing_tables=dynamodb.list_tables()['TableNames']
    if table_name in existing_tables:
        print(f" Table '{table_name}' already exists.")
        return
    
    print(f"creating DynamoDB '{table_name}'....")

    response=dynamodb.create_table(
        TableName=table_name,
        AttributeDefinitions=[
            {'AttributeName':'project_id',
             'AttributeType':'S'}
        ],
        KeySchema=[
            {
                'AttributeName':'project_id',
                'KeyType':'HASH'
            }
        ],
        ProvisionedThroughput={
    'ReadCapacityUnits': 5,
    'WriteCapacityUnits': 5
}

    )

    print("Waiting for table to be created")

    waiter=boto3.resource(
        'dynamodb',
        region_name=region,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key
    ).Table(table_name).wait_until_exists()

    print(f" Table '{table_name}' created successfully")

if __name__=="__main__":
    create_dynamodb_table()
