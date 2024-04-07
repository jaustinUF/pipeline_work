## Overview
This repository describes simple demo ELT pipeline projects. All of them:
  - use AWS services (Lambda, S3, and Glue)
  - require AWS keys to operate.
  - those with API calls probably require a current API key.

## Reddit pipeline 
<center><img src="assets/etl.png" alt="etl" width="75%" /></center>
The AWS services are orchestrated by Airflow via operator calls. Athena is not invoked by Airflow; it can be used to make a quick analysis. In a production environment, it's more likely the S3 parquet file would go into Redshift, via another Glue job if necessary.

## AWS 'pipelines'
Simple workflows consisting of lambdas triggered by loading a file into an S3 bucket, and a glue job triggerd directly by the lambda, or by the lambda saving a file into S3.
