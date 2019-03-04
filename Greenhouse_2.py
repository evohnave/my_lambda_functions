from __future__ import print_function
import boto3
import json
import decimal
import pandas as pd
import requests
from string import Template

"""
Requires a layer for pandas and requests.  Also, pandas requires the AWS numpy/scipy layer
Set up a SNS topic, NovettaJob.
Also, subscribe a SQS queue to the SNS topic
"""

def NovettaJobs():
    
    """
    Gets all jobs from the Novetta board on Greenhouse and returns
      them as a pandas dataframe
    """
    
    url = 'https://boards-api.greenhouse.io/v1/boards/novetta/jobs'

    # Get data
    r = requests.get(url)
    jobs = r.json()
    
    # need error checking on r
    # need to check the status_code
    #  200 - success
    #  204 - no data returned
    #  Others - probably bad
    
    # Put the job listings into a data frame
    joblist = jobs.get('jobs')
    df = pd.DataFrame.from_records(joblist)
    
    return df

# Helper class to convert a DynamoDB item to JSON.
class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            if o % 1 > 0:
                return float(o)
            else:
                return int(o)
        return super(DecimalEncoder, self).default(o)

def fix_kv(d: dict) -> dict:
    """
    Used to fix a JSON that has null or empty values before loading into DynamoDB
      Requires fix_list
    """
    for k, v in d.items():
        if isinstance(v, list):
            v = fix_list(v)
        elif isinstance(v, dict):
            v = fix_kv(v)
        elif v == '':
            d[k] = 'None'
    return d

def fix_list(l: list) -> list:
    for pos, item in enumerate(l):
        l[pos] = fix_kv(item)
    return l

def Get_Existing(Table):
    
    """
    Gets the existing id's from DynamoDB and returns them as a dataframe
      Input: Table - reference to a DynamoDB table
    """
    
    pe = 'id'
    
    response = Table.scan(
        ProjectionExpression=pe
        )
    
    # response -> dataframe
    existing = pd.DataFrame.from_records(response['Items'])
    
    return existing

def job_diff(old_df, new_df):
    
    """
    Determines the new job entries
      Returns a df of them
    """
    
    o = set(old_df['id'])
    n = set(new_df['id'])
    print(list(n.difference(o)))
    
    return new_df.loc[new_df['id'].isin(list(n.difference(o)))]

def Create_SNS_Text(num_jobs, new_jobs):
    """
    Creates the SNS text from new jobs.
      Returns a string (empty if no new jobs)
    """
    s = Template("<$url>, $title, $loc")

    textPart = f'Today, the total number of job postings is {num_jobs}.'
    textPart += f'\nThere have been {new_jobs.__len__()} new or updated jobs posted since yesterday.\n'
    sorted = new_jobs.sort_values(by='id', ascending=False)
    for index, row in sorted.iterrows():
            textPart += f'\n'
            textPart += s.safe_substitute(url=(row.absolute_url).strip(), 
                                          title=(row.title).strip(), 
                                          loc=(row.location['name']).strip())
    return textPart
    
def Publish_to_SNS(textPart):
    
    """
    Publishes textPart to SNS
    """

    sns = boto3.client(service_name="sns")
    topicArn = 'arn:aws:sns:us-east-1:079119988851:NovettaJobs'
 
    sns.publish(
        TopicArn = topicArn,
        Subject = "Today's New Jobs",
        Message =  textPart
    )
    
    return

def Write_new_to_DynamoDB(new_jobs, Table):
    
    """
    Write the new jobs to DynamoDB
       Input: Table - reference to a DynamoDB table
    """

    with Table.batch_writer() as batch:
        for index, row in new_jobs.iterrows():
            job = row.to_json()
            job = json.loads(job)
            print(job)
            batch.put_item(Item=fix_kv(job))
    return

def lambda_handler(event, context):
    """
    Lambda function for Novetta Jobs
    """
    
    print('Starting')
    # Set up connection to DynamoDB Table
    DynamoDB = boto3.resource('dynamodb', region_name='us-east-1', endpoint_url="https://dynamodb.us-east-1.amazonaws.com")
    Table = DynamoDB.Table('NovettaJobs')
    
    # Get a df of all jobs currently posted
    all_jobs = NovettaJobs()
    print('Got all posted jobs')
    
    # Get a df of existing jobs in DynamoDB
    old_jobs = Get_Existing(Table)
    print('Got old list of jobs')
    
    # Diff is the new postings
    new_jobs = job_diff(old_jobs, all_jobs)
    print('Figured out the difference')
    print(type(new_jobs))
    
    # If we have new jobs...
    if new_jobs.__len__() > 0:
        # Create the SNS Text
        textPart = Create_SNS_Text(all_jobs.__len__(), new_jobs)
        print('Created the SNS message')
        print(textPart)
    
        # Send the text to SNS
        Publish_to_SNS(textPart)
        print('Published to SNS')
    
        # Write new jobs to DynamoDB
        Write_new_to_DynamoDB(new_jobs, Table)
        print('Wrote to DynamoDB')
 
    return
