import os
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator

from format_to_parquet import format_to_parquet
from local_to_gcs import upload

gcs_workflow = DAG(
    "GCSIngestionDAG",
    schedule_interval="0 6 2 * *",
    start_date = datetime(2018,12,31),
    end_date = datetime(2021,1,1),
    catchup = True,
    max_active_runs = 2
    )

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
RAW_FILENAME = 'yellow_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.csv'
# RAW_FILENAME = 'yellow_tripdata_2021-01.csv'
URL_PREFIX = 'https://s3.amazonaws.com/nyc-tlc/trip+data/'
URL_TEMPLATE = URL_PREFIX + RAW_FILENAME
OUTPUT_FILE_TEMPLATE = AIRFLOW_HOME + '/raw/' + RAW_FILENAME.replace('yellow', 'output')
PARQUET_FILE = RAW_FILENAME.replace('.csv', '.parquet')
DATASET_NAME = RAW_FILENAME.replace('.csv', '')
PARQUET_FILE_TEMPLATE = OUTPUT_FILE_TEMPLATE.replace('.csv', '.parquet')

# Google cloud parameters
PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET_NAME = os.environ.get("GCP_GCS_BUCKET")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'trips_data_all')

DATA_SCHEMA = {    'VendorID': 'Int64',
    'tpep_pickup_datetime': 'object',
    'tpep_dropoff_datetime': 'object',
    'passenger_count': 'Int64',
    'trip_distance': 'float64',
    'RatecodeID': 'Int64',
    'store_and_fwd_flag': 'object',
    'PULocationID': 'Int64',
    'DOLocationID': 'Int64',
    'payment_type': 'Int64',
    'fare_amount': 'float64',
    'extra': 'float64',
    'mta_tax': 'float64',
    'tip_amount': 'float64',
    'tolls_amount': 'float64',
    'improvement_surcharge': 'float64',
    'total_amount': 'float64',
    'congestion_surcharge': 'float64'
}

DATE_COLS = []
    # 'tpep_pickup_datetime', 'tpep_dropoff_datetime']

with gcs_workflow:

    wget_task = BashOperator(
        task_id = 'wget',
        bash_command=f"curl -sSf {URL_TEMPLATE} > {OUTPUT_FILE_TEMPLATE}"
    )

    format_to_parquet_task = PythonOperator(
        task_id="format_to_parquet_task",
        python_callable=format_to_parquet,
        op_kwargs={
            "src_file": OUTPUT_FILE_TEMPLATE,
            "dtypes": DATA_SCHEMA,
            "datecols": DATE_COLS,
        },
    )

    local_to_gcs_task = PythonOperator(
        task_id="local_to_gcs_task",
        python_callable=upload,
        op_kwargs={
            "bucket": BUCKET_NAME,
            "dataset_name": DATASET_NAME,
            "local_pq_folder": OUTPUT_FILE_TEMPLATE.replace('.csv', '_parquet/'),
        },
    )
    bigquery_external_table_task = BigQueryCreateExternalTableOperator(
        task_id="bigquery_external_table_task",
        table_resource={
            "tableReference": {
                "projectId": PROJECT_ID,
                "datasetId": BIGQUERY_DATASET,
                "tableId": DATASET_NAME,
            },
            "externalDataConfiguration": {
                "sourceFormat": "PARQUET",
                "sourceUris": [f"gs://{BUCKET_NAME}/raw/{DATASET_NAME}_parquet/*.parquet"],
            },
        },
    )


    wget_task >> format_to_parquet_task >> local_to_gcs_task >> bigquery_external_table_task