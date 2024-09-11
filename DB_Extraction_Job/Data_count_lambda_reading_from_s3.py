import boto3
import json
import logging
import pandas as pd

s3 = boto3.client('s3')

def lambda_handler(event, context):
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']
    
    response = s3.get_object(Bucket=bucket, Key=key)
    data = json.loads(response['Body'].read())
    
    # Count users in each age_group and balance_group
    df = pd.DataFrame(data)
    age_group_counts = df['age_group'].value_counts().to_dict()
    balance_group_counts = df['balance_group'].value_counts().to_dict()
    
    # Log the result to CloudWatch
    logging.info(f"Age Group Counts: {age_group_counts}")
    logging.info(f"Balance Group Counts: {balance_group_counts}")
