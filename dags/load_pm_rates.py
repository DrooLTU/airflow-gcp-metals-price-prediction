from airflow import DAG, Dataset
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

from datetime import datetime, timedelta


TRANSFORMED_DATA = Dataset(f'file://opt/airflow/data/datasets/transformed_pm_rates.parquet')

default_args = {
    "owner": "Justinas",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "load_pm_rates",
    default_args=default_args,
    description="Load data via GCS to BigQuery warehouse when transformed data updates",
    schedule=[TRANSFORMED_DATA],
    start_date=datetime(2023, 10, 13),
    catchup=False,
)


load_to_gcs = LocalFilesystemToGCSOperator(
    task_id='load_to_gcs',
    bucket='t-m2s4-eu',
    src='/opt/airflow/data/datasets/transformed_pm_rates.parquet',
    dst='data/transformed.parquet',
    dag=dag
)

#PROLLY NEED A SENSOR HERE AS A GUARANTEE

load_to_bq = GCSToBigQueryOperator(
    task_id='load_to_bq',
    bucket='t-m2s4-eu',
    source_objects=['data/transformed.parquet'],
    destination_project_dataset_table='turing-m2-s4.precious_metals.rates',
    source_format='Parquet',
    write_disposition="WRITE_APPEND",
    schema_fields=[
        {'name': 'data_datetime', 'type': 'DATETIME'}, 
        {'name': 'EURUSD', 'type': 'FLOAT'},
        {'name': 'XAGUSD', 'type': 'FLOAT'},
        {'name': 'XAUUSD', 'type': 'FLOAT'},
        {'name': 'XPDUSD', 'type': 'FLOAT'},
        {'name': 'XPTUSD', 'type': 'FLOAT'}
    ],
    dag=dag
)

#PROLLY MOVE VIEW MATERIALISATION HERE AND SET UP A SENSOR FOR THE TABLE ON TRAINER DAG
# Convert to Variables maybe
project_id = "turing-m2-s4"
dataset_id = "precious_metals"
table_id = "latest_12_table"
view_id = "latest_12"


MATERIALIZE_VIEW_QUERY =(
    f'CREATE OR REPLACE TABLE `{project_id}.{dataset_id}.{table_id}` AS '
    f'SELECT * FROM `{project_id}.{dataset_id}.{view_id}`'
)

materialize_view = BigQueryInsertJobOperator(
    task_id="materialize_view",
    configuration={
        "query": {
            "query": MATERIALIZE_VIEW_QUERY,
            "useLegacySql": False,
            "priority": "BATCH",
        }
    },
    gcp_conn_id="google_cloud_default",
)


load_to_gcs >> load_to_bq >> materialize_view
