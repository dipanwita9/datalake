import requests
import os
from dotenv import load_dotenv
import boto3
import json
import time

load_dotenv()

# AWS Configuration
bucket_name = os.getenv("S3_BUCKET_NAME")
glue_db_name = os.getenv("GLUE_DB_NAME")
athena_db_name = os.getenv("ATHENA_DB_NAME")
athena_output_location = f"s3://{bucket_name}/athena_output_results"

api_key = os.getenv("API_KEY")
data_endpoint = os.getenv("DATA_ENDPOINT")


# AWS Clients
s3_client = boto3.client('s3')
glue_client = boto3.client('glue')
athena_client = boto3.client('athena')


# Create s3 bucket
def create_s3_bucket(bucket_name):
    try:
        s3_client.head_bucket(Bucket=bucket_name)
        print(f'Bucket {bucket_name} exists')
    except:
        print(f'Creating bucket {bucket_name}')

    try:
        s3_client.create_bucket(Bucket=bucket_name)
        print(f"Bucket {bucket_name} created successfully ")
    except Exception as e:
        print(f"Error creating bucket: {e}")

def convert_to_line_delimited_json(data):
    try:
        return "\n".join([json.dumps(rec) for rec in data])
    except Exception as e:
        print(f"Error converting to line-delimited JSON: {e}")
        return ""


def upload_data_to_s3(data):
    try:
        line_delimited_data = convert_to_line_delimited_json(data)
        file_key = os.getenv("S3_FILE_KEY")

        s3_client.put_object(
            Bucket=bucket_name,
            Key=file_key,
            Body=line_delimited_data
            )

        print(f"Data uploaded to s3://{bucket_name}/{file_key}")
    except Exception as e:
        print(f"Error uploading data to S3: {e}")


def create_glue_db(database_name):
    try:
        glue_client.create_database(
            DatabaseInput={
                'Name': database_name
            }
        )
        print(f"Database {database_name} created successfully")
    except Exception as e:
        print(f"Error creating database: {e}")


def create_glue_table():
    try:
        glue_client.create_table(
            DatabaseName=glue_db_name,
            TableInput={
                'Name': 'all_player_data',
                'Description': 'Player data',
                'StorageDescriptor': {
                    'Columns': [
                        {
                            'Name': 'PlayerID',
                            'Type': 'int'
                        },
                        {
                            'Name': 'FirstName',
                            'Type': 'string'
                        },
                        {
                            'Name': 'LastName',
                            'Type': 'string'
                        },
                        {
                            'Name': 'Position',
                            'Type': 'string'
                        },
                        {
                            'Name': 'College',
                            'Type': 'string'
                        },
                        {
                            'Name': 'Height',
                            'Type': 'string'
                        },
                        {
                            'Name': 'Weight',
                            'Type': 'string'
                        },
                        {
                            'Name': 'BirthDate',
                            'Type': 'string'
                        }
                    ],
                    'Location': f"s3://{bucket_name}/raw-data/",
                    'InputFormat': 'org.apache.hadoop.mapred.TextInputFormat',
                    'OutputFormat': 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat',
                    'Compressed': False,
                    'SerdeInfo': {
                        'SerializationLibrary': 'org.openx.data.jsonserde.JsonSerDe'
                    }
                },
                'TableType': 'EXTERNAL_TABLE'
            }
        )
        print("Table created successfully")
    except Exception as e:
        print(f"Error creating table: {e}")


def get_data():
    try:
        headers = {
            "Ocp-Apim-Subscription-Key": api_key
        }
        response = requests.get(data_endpoint, headers=headers)
        if response.status_code == 200:
            print("Data fetched successfully")
            return response.json()
        else:
            response.raise_for_status()
    except Exception as e:
        print(f"Error fetching data: {e}")
        return []
    
def config_athena():
    query = f"CREATE DATABASE IF NOT EXISTS {athena_db_name}"
    try:
        athena_client.start_query_execution(
            QueryString=query,
            QueryExecutionContext={'Database': glue_db_name},
            ResultConfiguration={'OutputLocation': athena_output_location}
        )
    except Exception as e:
        print(f"Error in Athena config {e}")
        
def run_sql():
    query = f"SELECT COUNT(*) FROM {glue_db_name}.all_player_data"
    try:
        athena_client.start_query_execution(
            QueryString=query,
            ResultConfiguration={'OutputLocation': athena_output_location}
        )
    except Exception as e:
        print(f"Error in fetching rows {e}")
    

def main():
    print("Setting up data lake for analytics")
    # Fetch data
    all_data = get_data()

    # Create S3 bucket
    create_s3_bucket(bucket_name)
    time.sleep(5) # wait for bucket to be created
    
    # Upload data to S3
    upload_data_to_s3(all_data)
    
    # Create Glue database
    create_glue_db(glue_db_name)
    
    # Create Glue table
    create_glue_table()
    
    # Configure Athena
    config_athena()
    
    # Run SQL queries on data using Athena
    run_sql()

if __name__ == "__main__":
        main()

