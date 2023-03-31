import boto3
from botocore.exceptions import ClientError
import json
import requests

def create_object(dynamodb):
    object = boto3.resource(dynamodb, endpoint_url="http://localhost:8001", region_name='us-east-1')
    return object


def create_movie_table(dynamodb):
    try:
        table = dynamodb.create_table(
            TableName='music',
            KeySchema=[
                {
                    'AttributeName': 'title',
                    'KeyType': 'HASH'  # Partition key
                },
                {
                    'AttributeName': 'artist',
                    'KeyType': 'RANGE'  # Sort key
                }
            ],
            AttributeDefinitions=[
                {
                    'AttributeName': 'title',
                    'AttributeType': 'S'
                },
                {
                    'AttributeName': 'artist',
                    'AttributeType': 'S'
                },
            ],
            ProvisionedThroughput={
                'ReadCapacityUnits': 10,
                'WriteCapacityUnits': 10
            }
        )
    except ClientError as error:
        if error.response['Error']['Code'] == 'ResourceInUseException':
            print(error)
    return table


def scan_tables(dynamodb):
    table = dynamodb.Table('music')
    response = table.scan()
    print(response['Items'])
    #print(response)


def delete_table(dynamodb,table_name):
    try:
        table = dynamodb.Table(table_name)
        table.delete()
    except Exception as e:
        print("Error deleting table. Exception displayed below : ")
        print(e)

def add_item(dynamodb):
    table = dynamodb.Table('music')
    response = table.put_item(
        Item={
            'title': 'test',
            'artist': 'aakash',
            'year': '2022',
            'web_url': 'https://test',
            'img_url': 'https://testimg'
        }
    )
    print(response)


def add_item_bulk(dynamodb,file_name):
    table = dynamodb.Table('music')
    with table.batch_writer() as batch:
        with open(file_name,'r') as file:
            data_to_parse = json.load(file)
            json_data = data_to_parse['songs']

            length_json=len(data_to_parse['songs'])

            for i in range(length_json):
                title = json_data[i]['title']
                artist = json_data[i]['artist']
                year = int(json_data[i]['year'])
                web_url = json_data[i]['web_url']
                img_url = json_data[i]['img_url']

                batch.put_item(
                    Item={
                        'title': title, 'artist': artist, 'year': year, 'web_url': web_url, 'img_url': img_url
                    }
                )


def read_item(dynamodb,title_name,artist):
    table = dynamodb.Table('music')
    response = table.get_item(
        Key={
            'title': title_name,
            'artist': artist
        }
    )
    print(json.dumps(response['Item']))


def upload_s3(file_name,bucket_name):
    with open(file_name, 'r') as file:
        data_to_parse = json.load(file)
        json_data = data_to_parse['songs']
        s3 = boto3.resource('s3')

        bucket = s3.Bucket(bucket_name)
        #create_upload_bucket(bucket_name)

        for music in json_data:
            response = requests.get(music['img_url'], stream=True)
            key = music['img_url']
            bucket.upload_fileobj(response.raw, key)


def create_upload_bucket(bucket_name):
    pass

def bucket_exists():
    s3 = boto3.resource('s3')
    response = s3.list_buckets()


def scenario():
    dynamodb = create_object('dynamodb')
    # movie_table = create_movie_table(dynamodb)
    # print("Table status:", movie_table.table_status)
    # scan_tables(dynamodb)
    # add_item(dynamodb)
    # add_item_bulk(dynamodb,'a2.json')
    # scan_tables(dynamodb)
    # read_item(dynamodb, 'test1', 'aakash1')
    # delete_table(dynamodb,'music')
    # create_upload_bucket('a1_json_images')
    # upload_s3('a1.json', 'a1_json_images')

if __name__ == '__main__':
    scenario()
