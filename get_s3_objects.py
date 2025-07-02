import boto3
from decouple import config
from botocore.exceptions import NoCredentialsError, PartialCredentialsError
# Use your AWS credentials (replace these with actual values)



def list_files_in_bucket(bucket_name):
    """
    List all files in an S3 bucket.

    :param bucket_name: Name of the S3 bucket
    """
    s3_client = boto3.client(
        's3',
        aws_access_key_id = config('aws_access_key_id'),
        aws_secret_access_key = config('aws_secret_access_key')
        )

    try:
        response = s3_client.list_objects_v2(Bucket=bucket_name)
        if 'Contents' in response:
            for obj in response['Contents']:
                print(f"File: {obj['Key']}")
        else:
            print(f"No files found in {bucket_name}")
    except Exception as e:
        print(f"An error occurred: {e}")

# Example usage
bucket_name= config('bucket_name')
list_files_in_bucket(bucket_name)