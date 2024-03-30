# See C:\Users\jaust\Desktop\AWS DE Projects\ELT service pipeline\Notes_info_doco.docx
# Trigger 'etl_source_to_parquet' Glue job and pass parameters
import json
from datetime import datetime, timedelta
import boto3

def lambda_handler(event, context):
    # partition arguments (for Glue job parquet file)
    year = str(datetime.now().year)
    month = str(datetime.now().month)
    day = str(datetime.now().day)
    hour = str(datetime.now().hour)
    
    # name of just-arrived object that triggered Lambda
    bucket_name = event['Records'][0]['s3']['bucket']['name']
    key_value = event['Records'][0]['s3']['object']['key']
    
    client = boto3.client('glue')
    glue_job_name = 'etl_source_to_parquet'
    
    client.start_job_run(
        JobName = glue_job_name,
        Arguments = {
            '--year_value': year,
            '--month_value': month,
            '--day_value': day,
            '--hour_value': hour,
            '--bucket': bucket_name,
            '--key': key_value
            }
        )
    print('etl_trigger_glue function')
    
    return {
        'statusCode': 200,
        'body': json.dumps('etl_source_to_parquet called')
    }
