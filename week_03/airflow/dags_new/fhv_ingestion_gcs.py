import os
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator

from format_to_parquet import format_to_parquet
from local_to_gcs import upload

gcs_workflow = DAG(
    "FHVIngestionDAG",
    schedule_interval="0 6 2 * *",
    start_date = datetime(2018,11,1),
    end_date = datetime(2021,1,1),
    catchup = True,
    max_active_runs = 2
    )

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
RAW_FILENAME = 'fhv_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.csv'
# RAW_FILENAME = 'fhv_tripdata_2019-01.csv'
# URL_PREFIX = 'https://s3.amazonaws.com/nyc-tlc/trip+data/'
URL_PREFIX = 'https://nyc-tlc.s3.amazonaws.com/trip+data/'
URL_TEMPLATE = URL_PREFIX + RAW_FILENAME
OUTPUT_FILE_TEMPLATE = f'{AIRFLOW_HOME}/raw/{RAW_FILENAME.replace("fhv", "output_fhv")}'
PARQUET_FILE = RAW_FILENAME.replace('.csv', '.parquet')
DATASET_NAME = RAW_FILENAME.replace('.csv', '')
PARQUET_FILE_TEMPLATE = OUTPUT_FILE_TEMPLATE.replace('.csv', '.parquet')

# Google cloud parameters
PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET_NAME = os.environ.get("GCP_GCS_BUCKET")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'trips_data_all')

DATA_SCHEMA = {

                'dispatching_base_num': 'object',
                'pickup_datetime': 'string',
                'dropoff_datetime': 'string',
                'PULocationID': 'Int64',
                'DOLocationID': 'Int64',
                'SR_Flag': 'object'
}
DATE_COLS = []
    # 'pickup_datetime','dropoff_datetime']

with gcs_workflow:

    wget_task = BashOperator(
        task_id = 'downLoad_raw_fhv_files',
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