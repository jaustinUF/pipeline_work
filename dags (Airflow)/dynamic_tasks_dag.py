# 'Dynamic tasks': call multiple tasks from 'for' loop (this test makes single call)
# built from 'find_the_file_from_binubuo.py'
# API: # https://rapidapi.com/codemonth/api/binubuo
# bucket = 'elt2-jja'
# 'key' : f'api_files/{file}.csv'

import requests
import boto3
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
data_file = 'local_data_file.csv'  # local file to pass data task1 > task2 
bucket = 'elt2-jja'

def file_save(file_name):
    url = f"https://binubuo.p.rapidapi.com/data/custom/{file_name}" # from func call in task
    querystring = {"locale":"US","csv":"True","rows":"5","cols":"first_name,last_name,address"}
    headers = {
	"X-RapidAPI-Key": "7de9129cfamsh827e4e4491e9d8dp1a708ajsn464837a3ec51",
	"X-RapidAPI-Host": "binubuo.p.rapidapi.com"
    }
    response = requests.get(url, headers=headers, params=querystring)
    # api call for .csv file
    if response.status_code == 200:
        with open(data_file, 'wb') as file:
            file.write(response.content)    # successful call, create local .csv file
        print("The CSV file has been saved successfully.")
    else:                                   # else show error
        print("Failed to retrieve data. Status code:", response.status_code)

def retrieve_upload_file(bucket, key):
    # IAM user 'external_access' keys
    s3 = boto3.resource(                    # configure s3 resource
        service_name='s3',
        region_name='us-east-1',
        aws_access_key_id='AKIAZ2NPLMMGFEGGYNOS',
        aws_secret_access_key='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'
    )
    # upload local file to s3 bucket
    s3.Bucket(bucket).upload_file(Filename=data_file, Key=key)

with DAG(
    dag_id='dynamic_tasks',
    description='API call/save s3 file in for loop',
    start_date=datetime(2024, 1, 27),
    # schedule_interval=timedelta(seconds=60),    # switch interval after test
    schedule_interval = '0 */1 * * *',        # every hour
    catchup=False 
) as dag:
    files = ('quick_fetch',) # tuple of files (all at same API endpoint)
    for file in files:
        task1 = PythonOperator(
            task_id = f'file_save_{file}',
            python_callable = file_save,
            op_kwargs = {'file_name' : file} # pass API file name to func
        )
        task2 = PythonOperator(
            task_id = f'retrieve_file_{file}',
            python_callable = retrieve_upload_file,
            op_kwargs = {'bucket' : bucket,'key' : f'api_files/{file}.csv'}
        )
            
        task1 >> task2    
    