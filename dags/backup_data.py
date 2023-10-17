from airflow import DAG

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
    description="Backs up all the necessary data.",
    schedule='None',
    catchup=False,
)

#DAG TO BACK UP ALL THE NECESSARY DATA
#SUPOSEDLY TO STORE ONLY CERTAIN AMMOUNT OF BACKUPS

#WHAT AND WHERE TO BACK UP
#1.TRANSFORMED DATA
#2.EXTRACTED DATA(?)
#3.BIG QUERY TABLE
#4.MODEL DATA(?)