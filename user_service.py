import hashlib
from boto3 import resource
import config
from boto3.dynamodb.conditions import Key , Attr

AWS_ACCESS_KEY_ID = config.AWS_ACCESS_KEY_ID
AWS_SECRET_ACCESS_KEY = config.AWS_SECRET_ACCESS_KEY
REGION_NAME = config.REGION_NAME
ENDPOINT_URL = config.ENDPOINT_URL

resource = resource(
    'dynamodb',
    endpoint_url = ENDPOINT_URL,
    aws_access_key_id     = AWS_ACCESS_KEY_ID,
    aws_secret_access_key = AWS_SECRET_ACCESS_KEY,
    region_name           = REGION_NAME
)

#define the table
def user_table():
    table = resource.create_table(
            TableName='Users',
            KeySchema=[
                {
                    'AttributeName': 'username',
                    'KeyType': 'HASH'
                }
            ],
            AttributeDefinitions=[
                {
                    'AttributeName': 'username',
                    'AttributeType': 'S'
                }
            ],
            ProvisionedThroughput=
                {
                    'ReadCapacityUnits': 5,
                    'WriteCapacityUnits': 5
                }
            )
    return table

user_table = resource.Table('Users')

#register user
def register_user(username , password):
    response = user_table.put_item(
        Item={
            'username': username,
            'password': hashlib.sha256(password.encode('utf-8')).hexdigest()
            }
        )
    return response

#login for user
def login_user(username , password):
    response = user_table.query(
            KeyConditionExpression=Key('username').eq(username),
            FilterExpression=Attr('password').eq(hashlib.sha256(password.encode('utf-8')).hexdigest()))
    return response