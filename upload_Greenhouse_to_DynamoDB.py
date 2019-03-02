"""
Used to upload data from Novetta's Greenhouse jobs to DynamoDB

Set up table NovettaJobs in DynamoDB in us-east-1
    one required field: id::number

Need to set up an IAM policy to allow the Lambda function to access DynamoDB

Also need to increase the timeout for the Lambda function from the default 3s

fix_kv() fixes the JSON for each job.  DynamoDB does not allow empty values, and '' or "" are
   considered empty.
   fix_list() is required by fix_kv()

"""

from __future__ import print_function           # in case you need to print() for debugging
import requests
import boto3

def fix_kv(d: dict) -> dict:
    for k, v in d.items():
        if isinstance(v, list):
            v = fix_list(v)
        elif isinstance(v, dict):
            v = fix_kv(v)
        elif v == '':
            d[k] = 'None'
    return(d)

def fix_list(l: list) -> list:
    for pos, item in enumerate(l):
        l[pos] = fix_kv(item)
    return l

def lambda_handler(event, context):
    url = 'https://boards-api.greenhouse.io/v1/boards/novetta/jobs'
    
    # Get data
    print('Getting Data')
    r = requests.get(url)
    print('Got Data')
    jobs = r.json()
    
    # Get the jobs -> now a list
    joblist = jobs.get('jobs')
    print('Got the joblist')
    
    print('Trying to get the dynamo resource')
    DynamoDB = boto3.resource('dynamodb', region_name='us-east-1', endpoint_url="https://dynamodb.us-east-1.amazonaws.com")
    print('Got the resource')
    Table = DynamoDB.Table('NovettaJobs')
    print('Got the table')
    
    with Table.batch_writer() as batch:
        for job in joblist:
            batch.put_item(Item=fix_kv(job))
    print('Wrote all items')
