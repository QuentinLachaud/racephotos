# Author: Quentin

import mysql.connector
from mysql.connector import Error
import pandas as pd
from mysql.connector import connect



def fetch_data_to_dataframe(query, host='', user='admin', password='TimAp777!', database='racephotos_user_details'):
    
    connection = connect(
        host='localhost',
        user='root',
        password='pass',
        database='users'  # Specify your database
    )
    
    try:
        df = pd.read_sql(query, connection)  # Using read_sql to fetch directly into DataFrame
    finally:
        connection.close()
    
    return df

def add_user_to_database(name, email, phone, dob):
    # connection = None
    try:
        # Connect to MySQL database
        connection = mysql.connector.connect(
            host='localhost',  # Change if MySQL is hosted elsewhere
            database='users', # Change to your database name
            user='root',  # Change to your MySQL username
            password='pass',
            port=3306  # Change to your MySQL password
        )

        if connection.is_connected():
            cursor = connection.cursor()

            # Insert query
            query = """INSERT INTO user_details (name, email, phone, dob) 
                    VALUES (%s, %s, %s, %s)"""
            values = (name, email, phone, dob)

            # Execute query and commit changes
            cursor.execute(query, values)
            connection.commit()
            print(f"User {name} added successfully.")

    except Error as e:
        print(f"Error while connecting to MySQL: {e}")
    finally:
        if connection:
            if connection.is_connected():
                cursor.close()
                connection.close()
                print("MySQL connection closed.")
    
    

def upload_to_s3(file_name, bucket_name, object_name=None):
    """
    Upload any file to s3 by providing its local path, 
    bucket name and object name (e.g. text_file.txt)
    """
    import boto3
    from botocore.exceptions import NoCredentialsError

    # If S3 object_name is not specified, use the file_name
    if object_name is None:
        object_name = file_name
    
    # Initialize an S3 client
    s3_client = boto3.client('s3')
    
    try:
        # Upload the file
        s3_client.upload_file(file_name, bucket_name, object_name)
        print(f"File {file_name} uploaded to {bucket_name}/{object_name}")
    except FileNotFoundError:
        print(f"The file {file_name} was not found.")
    except NoCredentialsError:
        print("Credentials not available.")