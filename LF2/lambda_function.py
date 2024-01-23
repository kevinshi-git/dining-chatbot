import json
import os

import boto3
from opensearchpy import OpenSearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth
import random

REGION = 'us-east-1'
HOST = 'search-restaurantsopensearch-csaxlocq3awmdah77rlcdvinl4.us-east-1.es.amazonaws.com'
INDEX = 'restaurant'


def lambda_handler(event, context):
    print('Received event: ' + json.dumps(event))

    sqs = boto3.client('sqs')
    response = sqs.receive_message(
    QueueUrl="https://sqs.us-east-1.amazonaws.com/386726448370/Q1",
    MaxNumberOfMessages=1,
    MessageAttributeNames=[
        'All'
    ],
    VisibilityTimeout=0,
    WaitTimeSeconds=0)

    print("response:", response)
    if "Messages" not in response:
        print("no message in queue!!")
        return {
            'statusCode': 200,
            'body': 'no message'
        }


    data = response['Messages'][0]
    cuisine = data['MessageAttributes'].get('Cuisine').get('StringValue')
    email = data['MessageAttributes'].get('Email').get('StringValue')
    results = query(cuisine)

    restaurant_suggestion = random.choice(results)
    temp_id = restaurant_suggestion.get("id")
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('yelp-restaurants')
    info = table.get_item(Key={'id': temp_id,})
    print("info: ", info)
    Rating = info["Item"]["rating"] if info["Item"]["rating"] else "blank"
    Name = info["Item"]["name"] if info["Item"]["name"] else "blank"
    RatingCount = info["Item"]["review_count"] if info["Item"]["review_count"] else "blank"
    Rating = info["Item"]["rating"] if info["Item"]["rating"] else "blank"
    Address = info["Item"]["address1"] if info["Item"]["address1"] else "blank"
    Zip = info["Item"]["zip_code"] if info["Item"]["zip_code"] else "blank"

    message = "The dinning bot recommends {}. It has {} reviews with an average {} rating. The address is: {}, {}.".format(Name, RatingCount, Rating, Address, Zip)
    print("message: ",message)
    
    
    #send email
    
    client = boto3.client('ses', region_name='us-east-1')
    response = client.send_email(
    Destination={
        'ToAddresses': [email]
    },
    Message={
        'Body': {
            'Text': {
                'Charset': 'UTF-8',
                'Data': message,
            }
        },
        'Subject': {
            'Charset': 'UTF-8',
            'Data': 'Dining Concierge Recommendation',
        },
    },
    Source='ks4164@columbia.edu'
    )
    print(response)
    
    receipt_handle = data['ReceiptHandle']
    sqs.delete_message(
    QueueUrl="https://sqs.us-east-1.amazonaws.com/386726448370/Q1",
    ReceiptHandle=receipt_handle)

    print("success???")

    return {
        'statusCode': 200,
        'headers': {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Headers': 'Content-Type',
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': '*',
        },
        'body': json.dumps({'results': results})
    }

def query(term):
    q = {'size': 5, 'query': {'multi_match': {'query': term}}}

    client = OpenSearch(hosts=[{
        'host': HOST,
        'port': 443
    }],
                        http_auth=get_awsauth(REGION, 'es'),
                        use_ssl=True,
                        verify_certs=True,
                        connection_class=RequestsHttpConnection)

    res = client.search(index=INDEX, body=q)
    print(res)

    hits = res['hits']['hits']
    results = []
    for hit in hits:
        results.append(hit['_source'])

    return results


def get_awsauth(region, service):
    cred = boto3.Session().get_credentials()
    return AWS4Auth(cred.access_key,
                    cred.secret_key,
                    region,
                    service,
                    session_token=cred.token)
