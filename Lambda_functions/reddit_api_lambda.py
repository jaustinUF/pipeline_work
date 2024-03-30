# To process:
#   this file would be renamed 'lambda_function.py
#   zipped into a .zip file with dependencies
#   uploaded to AWS Lambda
import praw              # simple access to Reddit's API
import datetime
import sys
import csv
import io
import re
import boto3

"""
Task in Airflow DAG. Takes in one command line argument of format YYYYMMDD. 
Script will connect to Reddit API and extract top posts from past day
with no limit. For a small subreddit like Data Engineering, this should extract all posts
from the past 24 hours.
"""

# reddit authentication Variables
SECRET = "(reddit secret)"
CLIENT_ID = "(ID)"

# Options for extracting data from PRAW (
SUBREDDIT = "dataengineering"
TIME_FILTER = "day"
LIMIT = None

# Fields to be extracted from Reddit.
POST_FIELDS = (
    "id",
    "title",
    "score",
    "num_comments",
    "author",
    "created_utc",
    "url",
    "upvote_ratio",
    "over_18",
    "edited",
    "spoiler",
    "stickied",
)

# Command line argument --> output file name; use current date if argv[1] not there
try:
    output_name = sys.argv[1]  # run-time is likely put in by Airflow
except IndexError:
    output_name = datetime.datetime.now().strftime("%Y%m%d")
except Exception as e:
    print(f"Error reading aya.argv[1]. Error {e}")
    sys.exit(1)
date_dag_run = datetime.datetime.strptime(output_name, "%Y%m%d")

# def main():
# this is the 'main' lambda function; calls all the other python functions.
def lambda_handler(event, context):
    """Extract Reddit data and load to CSV"""
    reddit_instance = api_connect()
    subreddit_posts_object = subreddit_posts(reddit_instance)
    extracted_data = extract_data(subreddit_posts_object)   # list of dictionary
    csv_out = data_to_csv_string(extracted_data)            # csv-format string
    load_to_csv(csv_out)                                    # string to file

def api_connect():
    """Connect to Reddit API"""
    try:
        instance = praw.Reddit(
            client_id=CLIENT_ID, client_secret=SECRET, user_agent="My User Agent"
        )
        return instance
    except Exception as e:
        print(f"Unable to connect to API. Error: {e}")
        sys.exit(1)

def subreddit_posts(reddit_instance):
    """Create posts object for Reddit instance"""
    try:
        subreddit = reddit_instance.subreddit(SUBREDDIT)
        posts = subreddit.top(time_filter=TIME_FILTER, limit=LIMIT)
        return posts
    except Exception as e:
        print(f"Issue subreddit_posts. Error: {e}")
        sys.exit(1)

def extract_data(posts):
    """Extract Data to list of dictionary"""
    list_of_items = []
    try:
        for submission in posts:
            to_dict = vars(submission)
            sub_dict = {field: to_dict[field] for field in POST_FIELDS}
            list_of_items.append(sub_dict)
    except Exception as e:
        print(f"Issue in extract_data. Error {e}")
        sys.exit(1)
    return list_of_items  # list of dictionary

# S3 bucket objects can not be appended, so two solutions to dealing with list of dictionary from 'extract_data:
#   upload the 'list_of_items' returned by 'extract_data' to an S3 object, then convert that file to a .csv file
#   convert 'list_of_items' to a .csv-formatted text string then upload to S3
# --> second option: process here
def remove_emojis(text):
    """Removes emoticon codes from strings using regex"""
    emoji_pattern = re.compile("["
       u"\U0001F600-\U0001F64F"  # emoticons
       u"\U0001F300-\U0001F5FF"  # symbols & pictographs
       u"\U0001F680-\U0001F6FF"  # transport & map symbols
       u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
       u"\U00002702-\U000027B0"
       u"\U000024C2-\U0001F251"
       "]+", flags=re.UNICODE)
    return emoji_pattern.sub(r'', text)

def data_to_csv_string(dict_list):
    """Convert extracted data (list of dictionary) to CSV-format string"""
    keys = dict_list[0].keys()      # all dictionaries have the same keys
    with io.StringIO() as output_string:
        dict_writer = csv.DictWriter(output_string, keys)
        dict_writer.writeheader()               # write header string
        for d in dict_list:                     # for each dictionary in list
            # clean dictionary via comprehension to remove emojis from string values
            cleaned_dict = {k: remove_emojis(v) if isinstance(v, str) else v for k, v in d.items()}
            dict_writer.writerow(cleaned_dict)  # write string from dictionary
        csv_string = output_string.getvalue()   # Get the CSV string from the StringIO object
    return csv_string

# needed for Lambda
# IAM user 'external_access' keys
s3 = boto3.resource(
    service_name='s3',
    region_name='us-east-1',
    aws_access_key_id='(AWS key)',
    aws_secret_access_key='(secret key)'
)

# string to S3: https://www.radishlogic.com/aws/s3/how-to-write-python-string-to-a-file-in-s3-bucket-using-boto3/
def load_to_csv(csv_to_output):
    """Save csv-format string to CSV file with date name"""
    object = s3.Object(
        bucket_name = 'raw-bucket-jja',
        key = f'reddit_project/api_files/{output_name}.csv'
    )
    object.put(Body=csv_to_output)
