from airflow import DAG
from airflow.providers.google.cloud.transfers.bigquery_to_gcs import (
    BigQueryToGCSOperator,
)

from airflow.providers.google.cloud.operators.bigquery import BigQueryDeleteTableOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryGetDataOperator
from airflow.providers.google.cloud.sensors.bigquery import BigQueryTableExistenceSensor

from custom.operators.train_model_operator import TrainModelOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

default_args = {
    "owner": "Justinas",
    "depends_on_past": False,
    "start_date": days_ago(1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "get_training_data",
    schedule_interval=None,
    default_args=default_args,
    catchup=False,
)


# Convert to Variables maybe
project_id = "turing-m2-s4"
dataset_id = "precious_metals"
table_id = "latest_12_table"

gcs_bucket = "t-m2s4-eu"
gcs_object = "latest_12.json"
local_filepath = "/data/views/latest_12.json"


check_table_existence = BigQueryTableExistenceSensor(
    task_id='check_table_existence',
    dataset_id=dataset_id,
    table_id=table_id,
)


# NOT NEEDED ATM, MAYBE USE FOR DATASET SCHEDULE???

# extract_and_save_to_gcs_task = BigQueryToGCSOperator(
#     task_id="extract_and_save_to_gcs",
#     source_project_dataset_table=f"{project_id}.{dataset_id}.{table_id}",
#     destination_cloud_storage_uris=[f"gs://t-m2s4-eu/data/latest_12.jsonl"],
#     export_format="JSONL",
#     gcp_conn_id="google_cloud_default",
#     dag=dag,
# )


get_data_from_bigquery = BigQueryGetDataOperator(
    task_id='get_data_from_bigquery',
    dataset_id=dataset_id,
    table_id=table_id,
    as_dict=True,
)


train_model_task = TrainModelOperator(
    task_id = 'train_model_task',
    datetime='{{ts}}',
    data_str='{{ task_instance.xcom_pull(task_ids="get_data_from_bigquery") }}',
)


delete_table = BigQueryDeleteTableOperator(
    task_id='delete_table',
    deletion_dataset_table=f'{dataset_id}.{table_id}',
)

# NOT NEEDED ATM

# download_task = GCSToLocalFilesystemOperator(
#     task_id="download_gcs_to_local",
#     bucket=gcs_bucket,
#     object_name=gcs_object,
#     filename=local_filepath,
#     gcp_conn_id="google_cloud_default",
#     dag=dag,
# )

check_table_existence >> get_data_from_bigquery >> train_model_task >> delete_table

if __name__ == "__main__":
    dag.cli()
