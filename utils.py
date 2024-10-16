# Author: Quentin

import mysql.connector
from mysql.connector import Error
import pandas as pd
from mysql.connector import connect
import boto3
import os
from botocore.exceptions import NoCredentialsError, ClientError
import io

import os

# Define the S3 bucket name
BUCKET_NAME = 'running-race-images'

# Initialize the S3 client
s3 = boto3.client('s3')

def upload_image_to_s3(file_path, user_id, event_id, bucket_name=BUCKET_NAME):
    try:
        # Count the number of images in the user's folder
        num_images = count_items_in_s3_folder(bucket_name, f"user-images/{user_id}")

        # Extract the file name from the path
        file_name = f"user_id_{user_id}_event_id_{event_id}_{generate_timestamp()}_{num_images + 1}.jpg"
        # Create a unique S3 key for the image
        s3_key = f"user-images/user_{user_id}/{file_name}"
        
        # Upload the file
        s3.upload_file(file_path, bucket_name, s3_key)
        print(f"Uploaded {file_name} to {bucket_name}/{s3_key}")
        return s3_key
    except FileNotFoundError:
        print(f"The file {file_path} was not found.")
    except NoCredentialsError:
        print("Credentials not available.")
    except ClientError as e:
        print(f"Failed to upload {file_name}: {e}")

def generate_timestamp():
    """
    Helper function to generate a timestamp for unique file names.
    :return: Timestamp string.
    """
    from datetime import datetime
    return datetime.now().strftime('%Y-%m-%d_%H-%M-%S')


def list_items_in_s3_folder(bucket_name=BUCKET_NAME, folder_prefix):
    """
    Counts the number of objects in a specific folder within an S3 bucket,
    excluding the folder itself.

    Parameters:
    bucket_name (str): The name of the S3 bucket
    folder_prefix (str): The folder path (prefix) in the S3 bucket (e.g., "users/user_00000002/")

    Returns:
    int: The number of objects in the folder, excluding the folder itself
    """
    try:
        # List the objects in the specified folder (prefix)
        response = s3.list_objects_v2(Bucket=bucket_name, Prefix=folder_prefix)

        # Check if there are any contents in the response
        if 'Contents' in response:
            # Filter out the folder itself (if it's included as an object)
            contents_only = [
                obj for obj in response['Contents'] 
                if obj['Key'] != folder_prefix
            ]

            # Return the count of actual contents
            return [item['Key'] for item in contents_only]
        else:
            # If no contents are found, return 0
            return 0
    except Exception as e:
        print(f"Error occurred: {e}")
        return None


def get_user_image_urls(user_id, bucket_name=BUCKET_NAME):
    """
    Retrieves the URLs of all images uploaded by a user.

    Parameters:
    user_id (str): The ID of the user

    Returns:
    list: A list of image URLs
    """
    # Get the list of items in the user's folder
    s3_items = list_items_in_s3_folder(bucket_name, folder_prefix=f"user-images/user_{user_id}/")

    return s3_items


def delete_image_from_s3(bucket_name, s3_key):
    """
    Deletes an image from the specified S3 bucket.

    Parameters:
    bucket_name (str): The name of the S3 bucket
    s3_key (str): The key (path) of the image to be deleted

    Returns:
    bool: True if the image was deleted successfully, False otherwise
    """
    try:
        s3.delete_object(Bucket=bucket_name, Key=s3_key)
        print(f"Successfully deleted {s3_key} from {bucket_name}.")
        return True
    except ClientError as e:
        print(f"Error deleting {s3_key}: {e}")
        return False
    except NoCredentialsError:
        print("Credentials not available.")
        return False



def count_items_in_s3_folder(bucket_name, folder_prefix):
    """
    Counts the number of objects in a specific folder within an S3 bucket,
    excluding the folder itself.

    Parameters:
    bucket_name (str): The name of the S3 bucket
    folder_prefix (str): The folder path (prefix) in the S3 bucket (e.g., "users/user_00000002/")

    Returns:
    int: The number of objects in the folder, excluding the folder itself
    """
    try:
        # List the objects in the specified folder (prefix)
        response = s3.list_objects_v2(Bucket=bucket_name, Prefix=folder_prefix)

        # Check if there are any contents in the response
        if 'Contents' in response:
            # Filter out the folder itself (if it's included as an object)
            contents_only = [
                obj for obj in response['Contents'] 
                if obj['Key'] != folder_prefix
            ]
            # Return the count of actual contents
            return len(contents_only)
        else:
            # If no contents are found, return 0
            return 0
    except Exception as e:
        print(f"Error occurred: {e}")
        return None
    

