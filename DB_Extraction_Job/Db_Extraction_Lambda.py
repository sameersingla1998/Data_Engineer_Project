#Lambda for DB Extraction Job
import os
import json
import psycopg2
from psycopg2 import sql
from psycopg2.extras import RealDictCursor, execute_values
import traceback
import sys
import boto3
import pandas as pd

def get_db_credentials(region_name,secret_name):
    """
    Function to get Database Secrets from Secret Manager
    Arguments:
        Region name: Name of the Region where Secret are placed
        SecretName: Name of secret in AWS where DB connection info is stored.
    """
    session=boto3.session.Session()
    client=session.client(service_name='secretsmanger',region_name='region_name')
    get_secret_value_response=client.get_secret_value(SecretId=secret_name)
    db_connection_dict=json.loads(get_secret_value_response['SecretString'])
    return db_connection_dict


# Function to get a connection to the PostgreSQL database
def get_db_connection(region_name,secret_name):
    try:
        database_detail=get_db_credentials(region_name,secret_name)
        connection = psycopg2.connect(
            host=database_detail['host'],
            database=database_detail['dbname'],
            user=database_detail['user'],
            password=database_detail['password'],
            port=database_detail['port']
        )
        return connection
    except psycopg2.Error as e:
        raise Exception(f"Error: Unable to connect to the database: {e}")
    
def upload_to_s3(df):
    s3 = boto3.client('s3')
    bucket_name = 'your-s3-bucket-name'
    file_name = 'transformed_data.json'
    
    s3.put_object(Bucket=bucket_name, Key=file_name, Body=df.to_json(orient='records'))

# Lambda handler function
def lambda_handler(event, context):
    connection = None
    region_name="us-east-2"
    secret_name="aws-db-user-secret"
    try:
        # Establish database connection
        connection = get_db_connection(region_name,secret_name)
        cursor = connection.cursor()
        query = "SELECT u.user_id, u.name, u.email, u.age, u.signup_date, b.balance, b.debt FROM users u JOIN bank_accounts b ON u.user_id = b.user_id"
        cursor.execute(query)
        records = cursor.fetchall()
        # data received from Db but right now db is not setup so dummy data is below

        data = [
                    (1, 'Alice', 'alice@example.com', 30, '2022-06-15', 100000.00, 5000.00),
                    (2, 'Bob', 'bob@example.com', 25, '2021-09-20', 150000.00, 10000.00),
                    (3, 'Charlie', 'charlie@example.com', 40, '2020-12-05', 250000.00, 20000.00),
                    (4, 'David', 'david@example.com', 35, '2019-04-22', 50000.00, 0.00),
                    (5, 'Eve', 'eve@example.com', 28, '2023-03-10', 300000.00, 15000.00)
                ]

        df = pd.DataFrame(data, columns=['user_id', 'name', 'email', 'age', 'signup_date', 'balance', 'debt'])
        connection.close()
        
        age_bins = [0, 20, 30, 40, 50, 100]  # Age ranges
        age_labels = ['0-20', '21-30', '31-40', '41-50', '50+']  # Age group labels
        df['age_group'] = pd.cut(df['age'], bins=age_bins, labels=age_labels)

        # Define balance groups in a simpler way
        balance_bins = [0, 50000, 100000, 200000, 300000, 500000]  # Balance ranges
        balance_labels = ['0-50K', '50K-100K', '100K-200K', '200K-300K', '300K+']  # Balance group labels
        df['balance_group'] = pd.cut(df['balance'], bins=balance_bins, labels=balance_labels)
        
        df = df[df['age'].between(0, 80)]
        # Return transformed data as JSON
        upload_to_s3(df)

    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }

