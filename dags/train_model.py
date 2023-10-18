from airflow import DAG
from airflow.providers.google.cloud.transfers.bigquery_to_gcs import (
    BigQueryToGCSOperator,
)

from airflow.providers.google.cloud.operators.bigquery import BigQueryDeleteTableOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryGetDataOperator
from airflow.providers.google.cloud.sensors.bigquery import BigQueryTableExistenceSensor
from airflow.models import Variable

from custom.operators.train_model_operator import TrainModelOperator
from airflow.utils.dates import days_ago
from datetime import timedelta


default_args = {
    "owner": "Justinas",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "train_model",
    description="Trains ML with latest 12 records from Big Query table",
    schedule='@hourly',
    default_args=default_args,
    catchup=False,
    start_date=days_ago(1),
)


project_id = Variable.get("gcp_default_project_id")
dataset_id = Variable.get("bq_main_dataset")
main_table_id = Variable.get("bq_main_table")
latest_table_id = Variable.get("bq_latest_12_table")
view_id = Variable.get("bq_latest_12_view")


check_table_existence = BigQueryTableExistenceSensor(
    task_id='check_table_existence',
    project_id=project_id,
    dataset_id=dataset_id,
    table_id=latest_table_id,
    timeout=60,
    mode="reschedule",
    poke_interval=60,
)


extract_and_save_to_gcs_task = BigQueryToGCSOperator(
    task_id="extract_and_save_to_gcs",
    source_project_dataset_table=f"{project_id}.{dataset_id}.{latest_table_id}",
    destination_cloud_storage_uris=[f"gs://t-m2s4-eu/data/latest_12.csv"],
    dag=dag,
)


get_data_from_bigquery = BigQueryGetDataOperator(
    task_id='get_data_from_bigquery',
    dataset_id=dataset_id,
    table_id=latest_table_id,
    as_dict=True,
)


train_model_task = TrainModelOperator(
    task_id = 'train_model_task',
    datetime='{{ts}}',
    data_str='{{ task_instance.xcom_pull(task_ids="get_data_from_bigquery") }}',
)


delete_table = BigQueryDeleteTableOperator(
    task_id='delete_table',
    deletion_dataset_table=f'{dataset_id}.{latest_table_id}',
)


check_table_existence >> [extract_and_save_to_gcs_task, get_data_from_bigquery] >> train_model_task >> delete_table

if __name__ == "__main__":
    dag.cli()
