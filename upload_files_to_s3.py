import boto3
from botocore.exceptions import NoCredentialsError, PartialCredentialsError
from decouple import config
from dotenv import load_dotenv
# Use your AWS credentials (replace these with actual values)
load_dotenv()

def upload_file_to_s3(file_name, bucket_name, object_name=None):
    """
    Upload a file to an S3 bucket.

    :param file_name: File to upload
    :param bucket_name: Bucket to upload to
    :param object_name: S3 object name. If not specified, file_name is used
    :return: True if file was uploaded, else False
    """
    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = file_name

    s3_client = boto3.client(
        's3',
        aws_access_key_id = config('aws_access_key_id'),
        aws_secret_access_key = config('aws_secret_access_key')
        )

    try:
        # Upload the file
        s3_client.upload_file(file_name, bucket_name, object_name)
        print(f"File {file_name} uploaded to {bucket_name}/{object_name}")
        return True
    except FileNotFoundError:
        print(f"The file {file_name} was not found")
        return False
    except NoCredentialsError:
        print("Credentials not available")
        return False
    except PartialCredentialsError:
        print("Incomplete credentials provided")
        return False
    except Exception as e:
        print(f"An error occurred: {e}")
        return False

# Example usage
if __name__ == "__main__":
    file_name = 'STL_MO_2025_KINGElvis.xlsx'  # Replace with your Excel file path
    bucket_name = 'etc-streamlit'  # Replace with your S3 bucket name
    object_name = 'STL_MO_2025_KINGElvis.xlsx'  # Replace with the desired S3 object name

    upload_file_to_s3(file_name, bucket_name, object_name)