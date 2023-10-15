from airflow import DAG, Dataset
from airflow.operators.python import PythonOperator

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

def _load_pm_rates():
    pass


load_into_bigquery = PythonOperator(
    task_id="load_into_bigquery",
    python_callable=_load_pm_rates,
    dag=dag,
)