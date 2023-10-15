from airflow import DAG, Dataset
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.local_to_bigquery import FileToBigQueryOperator

from datetime import datetime, timedelta

TRANSFORMED_DATA = Dataset(f'file://opt/airflow/data/datasets/transformed_pm_rates.json')

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
    description="Load data to warehouse when transformed data updates",
    schedule=[TRANSFORMED_DATA],
    catchup=False,
)

load_json_task = FileToBigQueryOperator(
    task_id='load_json',
    file_path='/opt/airflow/data/datasets/transformed_pm_rates.json',
    destination_project_dataset_table='turing-m2-s4.precious-metals.rates',
    schema_fields=[
        {'name': 'timestamp', 'type': 'TIMESTAMP'}, 
        {'name': 'EURUSD', 'type': 'FLOAT'},
        {'name': "XAGUSD", 'type': 'FLOAT'},
        {'name': "XAUUSD", 'type': 'FLOAT'},
        {'name': "XPDUSD", 'type': 'FLOAT'},
        {'name': "XPTUSD", 'type': 'FLOAT'}
    ]
)


dag >> load_json_task
