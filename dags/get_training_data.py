from airflow import DAG
from airflow.providers.google.cloud.transfers.bigquery_to_gcs import (
    BigQueryToGCSOperator,
)

from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryGetDataOperator

from custom.operators.train_model_operator import TrainModelOperator
from airflow.utils.dates import days_ago



dag = DAG(
    "get_training_data",
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
)


# Define the parameters for the BigQueryGetDataOperator
project_id = "turing-m2-s4"
dataset_id = "precious_metals"
table_id = "latest_12_table"

# SQL query to fetch data from the view

gcs_bucket = "t-m2s4-eu"
gcs_object = "latest_12.json"
local_filepath = "/data/views/latest_12.json"


MATERIALIZE_VIEW_QUERY =(
    f'CREATE OR REPLACE TABLE `{project_id}.{dataset_id}.latest_12_table` AS '
    f'SELECT * FROM `{project_id}.{dataset_id}.latest_12`'
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


extract_and_save_to_gcs_task = BigQueryToGCSOperator(
    task_id="extract_and_save_to_gcs",
    source_project_dataset_table=f"{project_id}.{dataset_id}.{table_id}",
    destination_cloud_storage_uris=[f"gs://t-m2s4-eu/data/latest_12.jsonl"],
    export_format="JSONL",
    gcp_conn_id="google_cloud_default",
    dag=dag,
)


get_data_from_bigquery = BigQueryGetDataOperator(
    task_id='get_data_from_bigquery',
    dataset_id=dataset_id,
    table_id=table_id,
    gcp_conn_id="google_cloud_default",
    as_dict=True,
)


train_model_task = TrainModelOperator(
    task_id = 'train_model_task',
    datetime='{{ds}}',
    data='{{ task_instance.xcom_pull(task_ids="get_data_from_bigquery") }}',
)

# download_task = GCSToLocalFilesystemOperator(
#     task_id="download_gcs_to_local",
#     bucket=gcs_bucket,
#     object_name=gcs_object,
#     filename=local_filepath,
#     gcp_conn_id="google_cloud_default",
#     dag=dag,
# )

materialize_view >> extract_and_save_to_gcs_task
extract_and_save_to_gcs_task >> get_data_from_bigquery >> train_model_task

if __name__ == "__main__":
    dag.cli()
