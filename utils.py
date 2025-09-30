from urllib.parse import urlparse, unquote
import os 
from dotenv import load_dotenv
import boto3
from botocore.config import Config
load_dotenv()
def format_state_codes(input_str):
    codes = input_str.split(';')
    quoted = [f"'{code}'" for code in codes]
    return ', '.join(quoted)


def get_s3_client():
    AWS_ACCESS_KEY = os.getenv('AWS_ACCESS_KEY')
    AWS_SECRET_KEY = os.getenv('AWS_SECRET_KEY')
    AWS_REGION = os.getenv("REGION_NAME")
    return boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY,
        region_name=AWS_REGION,
        config=Config(signature_version="s3v4")
    )

def sign_link(s3_client, bucket_name, url, minutes=10):
    if not url:
        print(f"Skipping signing due to empty URL")
        return None

    object_key = s3_key_from_url(url)

    if not object_key:
        print(f"Skipping signing due to empty object key for URL: {url}")
        return None
    return s3_client.generate_presigned_url(
        "get_object",
        Params={
            "Bucket": bucket_name,
            "Key": object_key
        },
        ExpiresIn=60 * minutes
    )
def s3_key_from_url(url: str) -> str:
    parsed_url = urlparse(url)
    return unquote(parsed_url.path.lstrip('/'))

def chunk_list(lst, size):
        for i in range(0, len(lst), size):
            yield lst[i:i + size] 
