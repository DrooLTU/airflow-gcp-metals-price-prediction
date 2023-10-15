from airflow import DAG, Dataset
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

from datetime import datetime, timedelta


TRANSFORMED_DATA = Dataset(f'file://opt/airflow/data/datasets/transformed_pm_rates.parquet')

default_args = {
    "owner": "Justinas",
    "depends_on_past": False,
    "start_date": datetime(2023, 10, 13),
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
        {'name': 'timestamp', 'type': 'TIMESTAMP'},
        {'name': 'data_datetime', 'type': 'DATETIME'}, 
        {'name': 'EURUSD', 'type': 'FLOAT'},
        {'name': 'XAGUSD', 'type': 'FLOAT'},
        {'name': 'XAUUSD', 'type': 'FLOAT'},
        {'name': 'XPDUSD', 'type': 'FLOAT'},
        {'name': 'XPTUSD', 'type': 'FLOAT'}
    ],
    dag=dag
)


load_to_gcs >> load_to_bq
