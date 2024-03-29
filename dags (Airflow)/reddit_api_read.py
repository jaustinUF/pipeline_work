# Pipeline thats run by Airflow, using AWS services
#   .csv file from reddit
#       from API call made by AWS Lambda, invoked in 'reddit_api_read'
#       to S3 bucket
#   .csv file from S3 bucket
#       picked up (by Glue) in 'transform_to_parquet'
#       transformed into parquet file
#       to S3 bucket
#   secuity provided by Airflow connection configuration.

from datetime import datetime
from airflow.models import DAG
from airflow.providers.amazon.aws.operators.lambda_function import LambdaInvokeFunctionOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator

with DAG(
    dag_id='reddit_read',
    schedule_interval='@daily',
    start_date=datetime(2023, 11, 18),
    catchup=False
) as dag:
	# reddit api call via Lambda
    # clean data, format to csv, to .csv file in bucket    
    reddit_api_read = LambdaInvokeFunctionOperator(
        task_id = 'read_reddit_api',
        function_name = 'reddit_api_call',
        invocation_type = 'RequestResponse',
        aws_conn_id = "s3_conn"
    ) 

    # data transform via Glue
    # get .csv file, clean data/add date column, to .parquet file
    transform_to_parquet = GlueJobOperator(
        task_id = 'to_parquet',     
        job_name = 'Reddit_data_to_df_show',
        script_location = 's3://raw-bucket-jja/reddit_project/scripts/',
        retry_limit  = 0,
        region_name = 'us-east-1',
        s3_bucket = 'raw-bucket-jja',
        iam_role_name = 'AWSGlueServiceRoleDefault',
        aws_conn_id = "s3_conn"
    )

    reddit_api_read >> transform_to_parquet
        
    











