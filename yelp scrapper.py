import json
import boto3
import requests
from decimal import Decimal
import pandas as pd
from time import sleep
import datetime
import collections
from elasticsearch import Elasticsearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth
import pickle
from urllib2 import HTTPError
from urllib import quote
from urllib import urlencode


#changed access and secret key for submission 
access_key = ""
secret_key = ""

dynamodb = boto3.resource('dynamodb', region_name='us-east-1', aws_access_key_id=access_key,  aws_secret_access_key = secret_key)
table = dynamodb.Table('yelp-restaurants')
# Using the Yelp API along with our yelp API key
API_KEY= ''  


# API constants, you shouldn't have to change these.
API_HOST = 'https://api.yelp.com'
SEARCH_PATH = '/v3/businesses/search'
BUSINESS_PATH = '/v3/businesses/'  # Business ID will come after slash.


def request(host, path, api_key, url_params=None):
    url_params = url_params or {}
    url = '{0}{1}'.format(host, quote(path.encode('utf8')))
    headers = {
        'Authorization': 'Bearer %s' % api_key,
    }

    print(u'Querying {0} ...'.format(url))

    response = requests.request('GET', url, headers=headers, params=url_params)

    return response


def flatten(d, parent_key='', sep='_'):
    items = []
    for k, v in d.items():
        new_key = k
        if isinstance(v, collections.MutableMapping):
            items.extend(flatten(v, new_key, sep=sep).items())
        else:
            items.append((new_key, v))
    return dict(items)


client = boto3.client('dynamodb', region_name='us-east-1', aws_access_key_id=access_key,  aws_secret_access_key = secret_key)
list_float=['rating', 'latitude', 'longitude', 'distance']

from decimal import Decimal
def convert_floats(item,list_float=list_float):
    for var in item:
        if var in list_float:
            item[var]=Decimal(str(item[var]))
    return item

# Creating the final list of dictionaries for the DynamoDB
cols = ['categories', 'id', 'name', 'address1', 'latitude', 'longitude', 'review_count', 'rating', 'zip_code']
final_list = []
cuisines = ['mediterranean', 'italian', 'indian', 'chinese', 'mexican', 'french']
for cuisine in cuisines:
  # add a pause
  sleep(0.5)
  for o in range(0, 1000, 50):
    url_params = {
      'term': 'restaurants',
      'location': 'New York City',
      'categories': cuisine,
      'offset': o,
      'limit': 50
    }
    print("Running for cuisine {} at offset {}".format(cuisine,o))
    response = request(API_HOST, SEARCH_PATH, API_KEY, url_params=url_params).json().get("businesses")
    sleep(0.5)
    for x in response:
      data = flatten(x)
      data = convert_floats(data)
      data2 = { your_key: data[your_key] for your_key in cols }
      data2['categories'] = cuisine
      data2['Timestamp'] = datetime.datetime.now().strftime("%d-%m-%Y %H:%M:%S")
      final_list.append(data2)

# Inserting the final dictionaries into the DynamoDB table
i = 0
table = dynamodb.Table('yelp-restaurants')
for entry in final_list:
  response = table.put_item(Item = entry)
  i+=1
  if i %500 ==0:
    print(i)
    sleep(0.1)

#save data for opensearch
es_list = []
for entry in final_list:
  r_name = entry.get("name")
  r_id = entry.get("id")
  r_cuisine = entry.get("categories")
  es_dict = {"cuisine":r_cuisine, "id":r_id}
  es_list.append(es_dict)

with open("test", "wb") as fp:   #Pickling
  pickle.dump(es_list, fp)
print("PICKED DATA !!!!")
