import shutil
import os

from airflow import DAG

from airflow.providers.google.cloud.transfers.bigquery_to_gcs import BigQueryToGCSOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowException

from datetime import datetime, timedelta


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
    "backup_data",
    default_args=default_args,
    description="Backups all the necessary data.",
    schedule=timedelta(hours=6),
    catchup=False,
)

output_path = '/opt/airflow/backups/'
zip_file_name = 'data_backup'
output_zip_file = os.path.join(output_path, f'{zip_file_name}.zip')

def _zip_folder(folder_path:str = '/opt/airflow/data', output_path:str = output_path, filename:str = zip_file_name):
    """
    Compress a folder into zip file.

    Args:
    folder_path: Path to the folder to be compressed.
    output_zip: Path where to store the result.
    filename: Name of the compressed file WITHOUT extension.
    """
    if not os.path.exists(folder_path):
        raise AirflowException(f"The folder {folder_path} does not exist.")
    
    if not os.path.exists(output_path):
        print(f'Creating the folder {output_path}.')
        os.makedirs(output_path)

    output_zip = os.path.join(output_path, filename)

    shutil.make_archive(output_zip, 'zip', folder_path)



# Convert to Variables maybe
project_id = "turing-m2-s4"
dataset_id = "precious_metals"
table_id = "rates"

#COLDLINE VERSIONED BUCKET (20 versions, 90 days expirity)
#NO NEED TO CARE ABOUT TIMESTAMPS AND HANDLING BACKUP COUNT
bucket = "t-m2s4-backups"

bigquery_backup_task = BigQueryToGCSOperator(
    task_id="bigquery_backup_task",
    source_project_dataset_table=f"{project_id}.{dataset_id}.{table_id}",
    destination_cloud_storage_uris=[f"gs://{bucket}/bq/bq_backup.csv"],
    dag=dag,
)


zip_data = PythonOperator(
    task_id='zip_data',
    python_callable=_zip_folder,
    dag=dag,
)


data_backup_task = LocalFilesystemToGCSOperator(
    task_id='data_backup_task',
    bucket=bucket,
    src=output_zip_file,
    dst='data/data_backup.zip',
    gzip=True,
    mime_type='application/zip',
    dag=dag
)

[bigquery_backup_task, zip_data] 
zip_data >> data_backup_task