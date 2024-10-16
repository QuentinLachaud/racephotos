# Author: Quentin

import mysql.connector
from mysql.connector import Error
import pandas as pd
from mysql.connector import connect
import boto3
import os
from botocore.exceptions import NoCredentialsError, ClientError
import io

# AWS client setup 
s3 = boto3.client('s3')


def upload_image_to_s3(file_content, user_id, bucket_name, file_name=None):
    """
    Uploads an image to S3.
    :param file_content: File data (bytes) to upload.
    :param user_id: Unique identifier for the user.
    :param bucket_name: S3 bucket name.
    :param file_name: Optional custom file name.
    :return: S3 key of the uploaded image.
    """
    try:
        # Generate a file name if not provided
        if not file_name:
            file_name = f"{user_id}_{generate_timestamp()}.jpg"
        
        # S3 key will be structured by user ID
        s3_key = f"users/{user_id}/{file_name}"
        
        # Upload file to S3
        s3.put_object(Bucket=bucket_name, Key=s3_key, Body=file_content, ContentType='image/jpeg')
        return s3_key
    
    except ClientError as e:
        print(f"Error uploading image to S3: {e}")
        return None
    except NoCredentialsError:
        print("AWS credentials not available.")
        return None

def generate_timestamp():
    """
    Helper function to generate a timestamp for unique file names.
    :return: Timestamp string.
    """
    from datetime import datetime
    return datetime.now().strftime('%Y-%m-%d_%H-%M-%S')

