import boto3
import os
import botocore
import awswrangler as wr
import pandas as pd
from datetime import datetime


class S3Manager():
    def __init__(self, aws_profile: str = None, region: str = "ap-southeast-2"):
        """
        If aws_profile is set, boto3 will load credentials from ~/.aws/credentials[aws_profile].
        Otherwise it falls back to env vars AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY,
        then to IAM role (if on EC2/Lambda/ECS), etc.
        """
        if aws_profile:
            session = boto3.Session(profile_name=aws_profile, region_name=region)
            self.s3 = session.client("s3")
        else:
            # these must be set in your shell / CI environment:
            aws_key    = os.getenv("AWS_ACCESS_KEY_ID")
            aws_secret = os.getenv("AWS_SECRET_ACCESS_KEY")
            if not aws_key or not aws_secret:
                raise RuntimeError(
                    "AWS credentials not found in environment. "
                    "Set AWS_ACCESS_KEY_ID & AWS_SECRET_ACCESS_KEY, "
                    "or pass aws_profile to __init__."
                )
            self.s3 = boto3.client(
                "s3",
                aws_access_key_id=aws_key,
                aws_secret_access_key=aws_secret,
                region_name=region
            )



    def Create_Bucket(self, bucket_name, region = 'ap-southeast-2'):
        """create bucket"""
        
        try:
            # Create S3 bucket
            self.s3.create_bucket(
                Bucket=bucket_name,
                CreateBucketConfiguration={
                    'LocationConstraint': region
                }
            )
            print(f'S3 bucket "{bucket_name}" created successfully in region {region}.')
        except Exception as e:
            print(f'Error: {e}')


    def List_Buckets(self):
        """List buckets"""
        response = self.s3.list_buckets()
        
        # Output the bucket names
        print('Existing buckets:')
        for bucket in response['Buckets']:
            print(f'  {bucket["Name"]}')


    def upload_file(self, file_name, bucket, object_name=None):
        """Upload a file to an S3 bucket"""
    
    
        # If S3 object_name was not specified, use file_name
        if object_name is None:
            object_name = os.path.basename(file_name)
        
        try:
            self.s3.upload_file(file_name, bucket, object_name)
            print(f'"{file_name}" is uploaded into bucket "{bucket}"')
        except Exception as e:
            print(e)
            return False
        

    def list_files(self, bucket_name):
        """List files in an S3 bucket and return in a list"""
        
        try:
            s3objects = self.s3.list_objects_v2(Bucket=bucket_name)
            #count = 0
            print(f"There are totoal {len(s3objects['Contents'])} files in S3 Bucket {bucket_name} ")
            if s3objects['KeyCount'] > 0:
                print('Listing objects ...')
                for s3object in s3objects['Contents']:
                    print(f"{s3object['Key']}")

        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "NoSuchBucket":
                print("Error: Bucket does not exist!!")
            elif e.response['Error']['Code'] == "InvalidBucketName":
                print("Error: Invalid Bucket name!!")
            elif e.response['Error']['Code'] == "AllAccessDisabled":
                print("Error: You do not have access to the Bucket!!")
            else:
                raise


    def list_s3_objects(self, bucket_name):
        """List objects in the bucket and return their keys in a list"""
        
        object_keys = []
        try:
            response = self.s3.list_objects_v2(Bucket=bucket_name)
            for obj in response.get('Contents', []):
                object_keys.append(obj['Key'])
            while response.get('IsTruncated', False):
                response = self.s3.list_objects_v2(Bucket=bucket_name, ContinuationToken=response['NextContinuationToken'])
                for obj in response.get('Contents', []):
                    object_keys.append(obj['Key'])
        except Exception as e:
            print(f"Error: {e}")
        return object_keys
        

    def delete_file(self, file_name, bucket_name):
        """Delete a file in an S3 bucket"""

        # Delete the bucket object
        try:
            if file_name in self.list_s3_objects(bucket_name):
                
                print('Deleting object ...')
                # Delete the object from bucket
                self.s3.delete_object(Bucket=bucket_name, Key=file_name)
                #print(response)
                print(f"{file_name} is deleted")
                return True 
            
            else: print("No such file!")           
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "AccessDenied":
                print("Error: Access denied!!")
            elif e.response['Error']['Code'] == "InvalidBucketName":
                print("Error: Invalid Bucket name!!")
            elif e.response['Error']['Code'] == "NoSuchBucket":
                print("Error: Bucket does not exist!!")
            elif e.response['Error']['Code'] == "AllAccessDisabled":
                print("Error: You do not have access to the Bucket!!")
            else:
                raise
            return False 


    def download_file(self, object_name, bucket_name, file_name = None):
        """Download a file from a bucket with or without naming"""

        try:
            if object_name in self.list_s3_objects(bucket_name):
                
                print('Downloading object ...')
                if file_name == None: file_name =object_name
                self.s3.download_file(bucket_name, object_name, file_name)
                print(f"{file_name} is downloaded")

            else: print("No such file!")
            
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "AccessDenied":
                print("Error: Access denied!!")
            elif e.response['Error']['Code'] == "InvalidBucketName":
                print("Error: Invalid Bucket name!!")
            elif e.response['Error']['Code'] == "NoSuchBucket":
                print("Error: Bucket does not exist!!")
            elif e.response['Error']['Code'] == "AllAccessDisabled":
                print("Error: You do not have access to the Bucket!!")
            else:
                print(e)


    def write_parquet(self, dataframe, key, timestamp = False, bucket_name = None):
        """Write dataframe as parquet in bucket"""
        
        if bucket_name == None: bucket_name = 'xtf-data'
        if timestamp:
            # Get current datetime
            now = datetime.now()
            # Format as string
            now_str = now.strftime('%Y-%m-%d-%H-%M-%S')
            path = f's3://{bucket_name}/parquet/{key}/{now_str}'
        else: path = f's3://{bucket_name}/parquet/{key}'
    
        try: 
            wr.s3.to_parquet(dataframe, path)
            print(f'Your file successfully saved as {path}')
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "AccessDenied":
                print("Error: Access denied!!")
            elif e.response['Error']['Code'] == "InvalidBucketName":
                print("Error: Invalid Bucket name!!")
            elif e.response['Error']['Code'] == "NoSuchBucket":
                print("Error: Bucket does not exist!!")
            elif e.response['Error']['Code'] == "AllAccessDisabled":
                print("Error: You do not have access to the Bucket!!")
            else:
                raise

    def read_parquet(self, file_name, bucket_name = None):
        """Read parquet as dataframe in certain bucket parquet folder"""
        
        if bucket_name == None: bucket_name = 'xtf-data'
        try: 
            path = f's3://{bucket_name}/parquet/{file_name}'
            df = wr.s3.read_parquet(path)
            return df 
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "AccessDenied":
                print("Error: Access denied!!")
            elif e.response['Error']['Code'] == "InvalidBucketName":
                print("Error: Invalid Bucket name!!")
            elif e.response['Error']['Code'] == "NoSuchBucket":
                print("Error: Bucket does not exist!!")
            elif e.response['Error']['Code'] == "AllAccessDisabled":
                print("Error: You do not have access to the Bucket!!")
            else:
                raise

    def get_object_content(self, bucket_name, key):
        """Return the decoded text content of an S3 object."""
        try:
            response = self.s3.get_object(Bucket=bucket_name, Key=key)
            return response['Body'].read().decode('utf-8')
        except Exception as e:
            print(f"Error reading {key}: {e}")
            return None