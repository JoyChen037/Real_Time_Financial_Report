import boto3
from .database import dataframe_to_s3
from .database import read_from_s3
session = boto3.Session()
credentials = session.get_credentials()
s3_client = boto3.client('s3',
                         aws_access_key_id=credentials.access_key, 
                         aws_secret_access_key=credentials.secret_key, 
                         aws_session_token=credentials.token)