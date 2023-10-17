from airflow import DAG

from airflow.providers.google.cloud.transfers.bigquery_to_gcs import (
    BigQueryToGCSOperator,
)

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
    schedule='None',
    catchup=False,
)


#DAG TO BACK UP ALL THE NECESSARY DATA
#SUPOSEDLY TO STORE ONLY CERTAIN AMMOUNT OF BACKUPS


#WHAT AND WHERE TO BACK UP
#1.TRANSFORMED DATA (Zip?)
#2.EXTRACTED DATA(Zip?)
#3.BIG QUERY TABLE
#4.MODEL DATA(Zip?)

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
    destination_cloud_storage_uris=[f"gs://{bucket}/bq/backup.csv"],
    dag=dag,
)


