import boto3
from botocore import UNSIGNED
from botocore.config import Config
from botocore.exceptions import ClientError
from boto3.s3.transfer import S3Transfer
from boto3.exceptions import S3UploadFailedError
import os
import json

BUCKET_NAME = 'avpbucket'
FILE_TO_UPLOAD = '/tmp/file.txt'

def create_file_if_not_exists(file_path):
    if not os.path.isfile(file_path):
        with open(file_path, 'w') as f:
            f.write('This is a test file.') 

def find_files_with_extension(s3_client, bucket_name, prefix=''):
    paginator = s3_client.get_paginator('list_objects_v2')
    try:
        for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
            for content in page.get('Contents', []):
                key = content['Key']
                if key[-1] != '/':  
                    if '.' in os.path.basename(key):  
                        return key  
            for common_prefix in page.get('CommonPrefixes', []):
                sub_prefix = common_prefix['Prefix']
                found_file = find_files_with_extension(s3_client, bucket_name, sub_prefix)
                if found_file:  
                    return found_file
    except ClientError as e:
        if e.response['Error']['Code'] == 'AccessDenied':
            print(f"Access Denied when listing objects in bucket {bucket_name}")
        return None
    return None

def check_bucket_acl(s3_client, bucket_name):
    try:
        acl = s3_client.get_bucket_acl(Bucket=bucket_name)
        return True
    except ClientError as e:
        return False

def s3_operations(bucket_name, file_to_upload):
    results = {}
    s3_client = boto3.client('s3', config=Config(signature_version=UNSIGNED))

    create_file_if_not_exists(file_to_upload)

    results['acl listing'] = "Positive" if check_bucket_acl(s3_client, bucket_name) else "Negative"

    file_key = find_files_with_extension(s3_client, bucket_name)
    results['file listing'] = "Positive" if file_key else "Negative"
    results['file download'] = "Negative" if not file_key else "Not attempted"

    if file_key:
        local_file_path = file_key
        directory = os.path.dirname(local_file_path)

        if directory and not os.path.exists(directory):
            os.makedirs(directory)

        try:
            s3_client.download_file(bucket_name, file_key, local_file_path)
            results['file download'] = "Positive"
        except ClientError as e:
            results['file download'] = "Negative"
    
    try:
        s3_client.upload_file(file_to_upload, bucket_name, 'test_upload_file.txt')
        results['file upload'] = "Positive"
    except ClientError as e:
        results['file upload'] = "Negative" if e.response['Error']['Code'] == 'AccessDenied' else "Unknown error"
    except S3UploadFailedError as e:
        results['file upload'] = "Negative" 

    return {
        'statusCode': 200,
        'body': json.dumps(results),
        'headers': {
            'Content-Type': 'application/json'
        },
    }

def lambda_handler(event, context):
    response = s3_operations(BUCKET_NAME, FILE_TO_UPLOAD)
    return response
