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
    ) # https://airflow.apache.org/docs/apache-airflow-providers-amazon/stable/operators/lambda.html#howto-operator-lambdainvokefunctionoperator
    # https://airflow.apache.org/docs/apache-airflow-providers-amazon/stable/_api/airflow/providers/amazon/aws/operators/lambda_function/index.html#airflow.providers.amazon.aws.operators.lambda_function.LambdaInvokeFunctionOperator

    # data transform via Glue
    # get .csv file, (perhaps) clean data/add date column, to .parquet file
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
        
    











