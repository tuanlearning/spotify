import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import logging
from dotenv import load_dotenv
from google.cloud import bigquery
from google.cloud import storage
from airflow.operators.bash import BashOperator

def execute_dbt_model(model_name):
    return BashOperator(
            task_id=f'run_dbt_{model_name}',
            bash_command=f'cd /opt/spotify/spotify && dbt run --select {model_name}'
        )

def upload_to_gcs(bucket_name, source_file_name, destination_blob_name):
    try:
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)

        blob.upload_from_filename(source_file_name)
        logging.info(f"✅ Uploaded {source_file_name} to gs://{bucket_name}/{destination_blob_name}")
    except Exception as e:
        logging.info(f"Cannot uploaded {source_file_name} to gs://{bucket_name}/{destination_blob_name}, Error: {e}")

def init_bq_client():
    load_dotenv()
    bq_client = bigquery.Client()
    return bq_client

def create_external_table(bq_client, PROJECT_ID, DATASET_ID, TABLE_ID, uris):
    try:
        query = f"""
            CREATE OR REPLACE EXTERNAL TABLE `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`
            OPTIONS (
            format = 'CSV',
            uris = ['{uris}'],
            skip_leading_rows = 1
            );
        """
        bq_client.query(query)
        logging.info(f'✅ Successfully create table {PROJECT_ID}.{DATASET_ID}.{TABLE_ID}')
    except Exception as e:
        logging.error(f"Cannot create table: {e}")