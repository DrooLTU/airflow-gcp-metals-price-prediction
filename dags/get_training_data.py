from airflow import DAG
from airflow.providers.google.cloud.transfers.bigquery_to_gcs import (
    BigQueryToGCSOperator,
)
from airflow.providers.google.cloud.transfers.gcs_to_local import (
    GCSToLocalFilesystemOperator,
)
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryGetDataOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

# Define your DAG
dag = DAG(
    "get_training_data",
    schedule_interval=None,  # Set your desired schedule interval
    start_date=days_ago(1),  # Set the start date
    catchup=False,
)

def _preprocess_data(data):
    print(data)


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

preprocess_data_task = PythonOperator(
    task_id='preprocess_data',
    python_callable=_preprocess_data,
    op_kwargs={'data': '{{ task_instance.xcom_pull(task_ids="get_data_from_bigquery") }}'} # Passes the output of the previous task as an argument to this task
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
extract_and_save_to_gcs_task >> get_data_from_bigquery >> preprocess_data_task

if __name__ == "__main__":
    dag.cli()
