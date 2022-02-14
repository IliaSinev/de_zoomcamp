import os

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator


from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator, BigQueryInsertJobOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator

from google.cloud import storage


PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")

# dataset_file = "yellow_tripdata_2021-01.csv"
# dataset_url = f"https://s3.amazonaws.com/nyc-tlc/trip+data/{dataset_file}"
path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
# parquet_file = dataset_file.replace('.csv', '.parquet')
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'trips_data_all')
COLOR='fhv'


default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
}

def move_files(color):
    client = storage.Client()
    bucket = client.bucket(BUCKET)
    blobs = bucket.list_blobs(prefix='raw/')
    # print(f'blobs with prefix:{blobs}')
    # for b in blobs:
    #     print(f"next blob: {b.name.split('/')[1].split('_')[0]}")
    color_blobs = [x for x in blobs if x.name.split('/')[1].split('_')[0]==color]
    print(f'blobs to move: {color_blobs}')
    for blob in color_blobs:
        print(f'Moving {blob.name} to {color}/')
        bucket.rename_blob(blob, new_name=blob.name.replace(f"{blob.name.rsplit('/',1)[0]}/", f'{color}/'))

 
with DAG(
    dag_id="gcs_2_bq_dag",
    schedule_interval="@daily",
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=['dtc-de'],
) as dag:

    gcs_to_gcs_task = PythonOperator(
        task_id="gcs_to_gcs_task",
        python_callable=move_files,
        op_kwargs={
            "color": COLOR,
        },
    )

    gcs_to_bq_ext_task = BigQueryCreateExternalTableOperator(
    task_id="gcs_to_bq_ext_task",
    table_resource={
        "tableReference": {
            "projectId": PROJECT_ID,
            "datasetId": BIGQUERY_DATASET,
            "tableId": f"{COLOR}_2019-04_external_table",
        },
        "externalDataConfiguration": {
            "sourceFormat": "PARQUET",
            "sourceUris": [f"gs://{BUCKET}/{COLOR}/fhv_tripdata_2019-04*"],
        },
    },
)

    CREATE_PART_TBL_QUERY = f"CREATE OR REPLACE TABLE {PROJECT_ID}.{BIGQUERY_DATASET}.{COLOR}_2019-04_tripdata_partitoned \
                                PARTITION BY \
                                DATE(pickup_datetime) AS \
                                SELECT * except(pickup_datetime, dropoff_datetime), \
                                    CAST (pickup_datetime AS datetime) AS pickup_datetime, \
                                    CAST (dropoff_datetime AS datetime) AS dropoff_datetime \
                                FROM {BIGQUERY_DATASET}.{COLOR}_2019-04_external_table;"

    bq_ext_2_part_task = BigQueryInsertJobOperator(
        task_id="bq_ext_2_part_task",
        configuration={
            "query": {
                "query": CREATE_PART_TBL_QUERY,
                "useLegacySql": False,
            }
        }

)

    gcs_to_gcs_task >> gcs_to_bq_ext_task >> bq_ext_2_part_task
