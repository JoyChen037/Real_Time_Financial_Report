import pyarrow.parquet as pq
import s3fs
import pandas as pd
from io import BytesIO, StringIO

def dataframe_to_s3(s3_client, input_dataframe, bucket_name, filepath, format):
    """
    Uploads a DataFrame to an S3 bucket in the specified format (parquet or csv).

    Args:
        s3_client (boto3.client): The S3 client.
        input_dataframe (pd.DataFrame): The DataFrame to upload.
        bucket_name (str): The name of the S3 bucket.
        filepath (str): The path to the file within the bucket.
        format (str): The format of the file (parquet or csv).
    """
    if format == 'parquet':
        out_buffer = BytesIO()
        input_dataframe.to_parquet(out_buffer, index=False)
    elif format == 'csv':
        out_buffer = StringIO()
        input_dataframe.to_csv(out_buffer, index=False)

    s3_client.put_object(Bucket=bucket_name, Key=filepath, Body=out_buffer.getvalue())


def read_from_s3(s3_client, bucket_name, filepath, format):
    """
    Reads data from an S3 bucket in the specified format (parquet or csv).

    Args:
        s3_client (boto3.client): The S3 client.
        bucket_name (str): The name of the S3 bucket.
        filepath (str): The path to the file within the bucket.
        format (str): The format of the file (parquet or csv).

    Returns:
        pd.DataFrame: The DataFrame containing the data from the S3 file.
    """
    if format == 'parquet':
        s3_object = s3_client.get_object(Bucket=bucket_name, Key=filepath)
        df = pd.read_parquet(BytesIO(s3_object['Body'].read()))
    elif format == 'csv':
        s3_object = s3_client.get_object(Bucket=bucket_name, Key=filepath)
        df = pd.read_csv(StringIO(s3_object['Body'].read().decode('utf-8')))

    return df