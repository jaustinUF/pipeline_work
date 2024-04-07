## Overview
This repository describes simple demo ELT pipeline projects. All of them:
  - are orchestrated by Airflow: thus the DAG python file is the top-level code.
  - use AWS services (Lambda, S3, and Glue)
  - require AWS keys to operate.
  - those with API calls probably require a current API key.

## Reddit pipeline 
<center><img src="assets/etl.png" alt="etl" width="75%" /></center>
T the AWS services are orchestrated by Airflow via operator calls. Athena is not invoked by Airflow; it can be used to make a quick analysis. In a production environment, it's more likely the S3 parquet file would go into Redshift, via another Glue job if necessary. 
