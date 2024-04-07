import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# pickup current file name, use in path (instead of hardcodeed)
df = spark.read.csv("s3://raw-bucket-jja/reddit_project/api_files/20231120.csv")
# df work: data clean-up, add date column
df.write.parquet("s3://raw-bucket-jja/reddit_project/parquet_files/20231120.parquet")

job.commit()