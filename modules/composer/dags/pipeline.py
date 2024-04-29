from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import json
from modules.composer.apis.api import TomtomClient
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
import logging
import os, ast
import io

# Define the ETL functions
def extract():
    try:
        # Initialize Google Cloud Storage Hook
        gcs_hook = GoogleCloudStorageHook()

        # Define GCS bucket and object
        bucket_name = os.getenv('bucket_name')

        # Download coordinates file from GCS
        coordinates_bytes = gcs_hook.download(bucket_name=bucket_name, object_name='dags/data/coordinates_file.json')

        # Decode bytes to string
        coordinates_string = coordinates_bytes.decode('utf-8')

        # Load coordinates data from JSON string
        coordinates = json.loads(coordinates_string)

        client = TomtomClient()  # initialize the TomtomClient

        data_list = []
        for coordinate in coordinates.items():
            response = client.make_request_bounding_box(coordinate[1]["bbox"])
            data_list.append(response.json()) # append the response to the data list

        return data_list
    except Exception as e:
        print(f"Error occurred during data extraction: {str(e)}")
        exit(1)


def transform(extracted_data):
    try:
        incidents = pd.DataFrame(columns=["id", "geoType", "geoCoordinates", "from", "to", "startTime", "endTime", "roadNumbers", "length", "delay", "category"])
        
        extracted_data = str(extracted_data).replace("\'", "\"")
        list = ast.literal_eval(str(extracted_data))
        
        for data in list:
            for incident in data['incidents']:
                # Extracting information from each incident
                
                incidents["id"] = incident["properties"]["id"],
                incidents["geoType"] = incident["geometry"]["type"],
                incidents["geoCoordinates"] = incident["geometry"]["coordinates"],
                incidents["from"] = incident["properties"]["from"],
                incidents["to"] = incident["properties"]["to"],
                incidents["startTime"] = incident["properties"]["startTime"],
                incidents["endTime"] = incident["properties"]["endTime"],
                incidents["roadNumbers"] = incident["properties"]["roadNumbers"],
                incidents["length"] =  incident["properties"]["length"],
                incidents["delay"] = incident["properties"]["delay"],
                incidents["category"] = incident["properties"]["iconCategory"]
  
        return incidents.to_dict(orient='records')
    except Exception as e:
        logging.error("Error occurred during data transformation:", e)
        exit(1)


def load(data):
    try:
        data = str(data).replace("null", "None")
        data = eval(data)

        # Convert the dictionary to a DataFrame
        data_df = pd.DataFrame.from_dict(data)

        # Connect to Google Cloud Bucket
        gcs_hook = GoogleCloudStorageHook()

        # Define GCS bucket and object
        bucket_name = os.getenv('bucket_name')

        # Download coordinates file from GCS
        data_file_bytes = gcs_hook.download(bucket_name=bucket_name, object_name='dags/data/data_file.csv')

        # Decode bytes to string
        data_file_string = data_file_bytes.decode('utf-8-sig')

        # Load coordinates data from CSV string
        data_file = pd.read_csv(io.StringIO(data_file_string),sep=',', index_col="id")

        # Update data_file with values from data_df
        data_file.update(data_df)
        
        # merge the two dataframes
        data_file = pd.merge(data_file, data_df)

        # Reset the index
        data_file.reset_index(inplace=True)

        # Save the updated data_file to a CSV file
        csv_data = data_file.to_csv(index=False)

        #load to bucket
        gcs_hook.upload(bucket_name=bucket_name, object_name='dags/data/data_file.csv', data=csv_data)


    except (Exception) as error:
        print("Error loading data into file:", error)
        exit(1)

# Define the DAG

with DAG(
    dag_id="etl_process",
    start_date=datetime(2024, 1, 1),
    schedule_interval=timedelta(hours=1),  
    catchup=False,
) as dag:

    extract_task = PythonOperator(
        task_id='extract_data',
        python_callable=extract
    )

    transform_task = PythonOperator(
        task_id='transform_data',
        python_callable=transform,
        provide_context=True,  # Pass the context to the callable function
        op_args=['{{ ti.xcom_pull(task_ids="extract_data") }}'],
        dag=dag
    )

    load_task = PythonOperator(
        task_id='load_data',
        python_callable=load,
        provide_context=True,  # Pass the context to the callable function
        op_args=['{{ ti.xcom_pull(task_ids="transform_data") }}'],
        dag=dag
    )

    extract_task >> transform_task >> load_task
