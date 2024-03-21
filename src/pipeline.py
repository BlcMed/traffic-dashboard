from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import json
from src.api import TomtomClient
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
import logging
import psycopg2, os, ast

# Define the ETL functions
def extract():
    try:
        # Initialize Google Cloud Storage Hook
        gcs_hook = GoogleCloudStorageHook()

        # Define GCS bucket and object
        bucket_name = 'us-central1-mycomposer-0ccdd0a0-bucket'

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
        incidents = {}
        
        extracted_data = str(extracted_data).replace("\'", "\"")
        list = ast.literal_eval(str(extracted_data))
        
        for data in list:
            for incident in data['incidents']:
                # Extracting information from each incident
                incident_ = {
                    "id": incident["properties"]["id"],
                    "geo_type": incident["geometry"]["type"],
                    "geo_coordinates" : incident["geometry"]["coordinates"],
                    "from": incident["properties"]["from"],
                    "to": incident["properties"]["to"],
                    "startTime": incident["properties"]["startTime"],
                    "endTime": incident["properties"]["endTime"],
                    "roadnumbers": incident["properties"]["roadNumbers"],
                    "length":  incident["properties"]["length"],
                    "delay": incident["properties"]["delay"],
                    "category": incident["properties"]["iconCategory"]
                }
                incidents[incident_["id"]] = incident_

        incidents_json = json.dumps(incidents)
        return incidents_json
    except Exception as e:
        logging.error("Error occurred during data transformation:", e)
        exit(1)

def load(data):
    try:
        data = str(data).replace("null", "None")
        data = eval(data)

        # Connect to Google Cloud SQL
        conn = psycopg2.connect(
            host= os.getenv('DB_IP'),
            database= os.getenv('DB_NAME'),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD")
        )
        cursor = conn.cursor()

        # itterate through the dict of dict data and insert into the table
        for value in data.values():
            insert_query = """
                INSERT INTO traffic_incidents (id, geometry_type, geometry_coordinates, from_, to_, start_time, end_time, road_numbers, length, delay, category)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (id) DO UPDATE
                SET geometry_type = EXCLUDED.geometry_type,
                    geometry_coordinates = EXCLUDED.geometry_coordinates,
                    from_ = EXCLUDED.from_,
                    to_ = EXCLUDED.to_,
                    start_time = EXCLUDED.start_time,
                    end_time = EXCLUDED.end_time,
                    road_numbers = EXCLUDED.road_numbers,
                    length = EXCLUDED.length,
                    delay = EXCLUDED.delay,
                    category = EXCLUDED.category;
            """
            data = (value["id"], value["geo_type"], value["geo_coordinates"], value["from"], value["to"], value["startTime"], value["endTime"], value["roadnumbers"], value["length"], value["delay"], value["category"])
            cursor.execute(insert_query, data)
        

        # Commit the transaction and close connection
        conn.commit()
        cursor.close()
        conn.close()
            
        
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error loading data into database:", error)
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
