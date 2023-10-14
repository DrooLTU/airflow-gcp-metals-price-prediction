import os
from dotenv import load_dotenv

from airflow import DAG, macros
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor

from datetime import datetime, timedelta
from typing import List
import requests
import json


load_dotenv()


default_args = {
    'owner': 'Justinas',
    'depends_on_past': False,
    'start_date': datetime(2023, 10, 13),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

BASE_SYMBOL = 'USD'
SYMBOLS = ['EUR', 'XAU', 'XAG', 'XPD', 'XPT']

API_KEY = os.getenv("API_KEY")
if not API_KEY:
    API_KEY = Variable.get("pm_api_key")


base_url = 'https://api.metalpriceapi.com/v1/latest'

dag = DAG(
    'get_pm_rates',
    default_args=default_args,
    description='Get and store the precious metal rates from the API',
    schedule=None,
    catchup=False,
)


def _save_file(file_name, file_content, dir_path):
  """Saves a file to the specified directory.

  Args:
    file_name: The name of the file to save.
    file_content: The content of the file to save.
    dir_path: The directory to save the file to.
  """

  if not os.path.exists(dir_path):
    os.makedirs(dir_path)

  file_path = os.path.join(dir_path, file_name)

  with open(file_path, "w") as f:
    f.write(file_content)


def _extract_pm_rates(base:str, symbols:List[str], **kwargs) -> None:
    execution_date = kwargs["execution_date"]
    hour = execution_date.hour
    session = requests.Session()
    symbols_str = ','.join(symbols)
    url = f'{base_url}?api_key={API_KEY}&base={base}&currencies={symbols_str}'
    response = session.get(url)
    print(response)
    if response.status_code == 200:
        file_name = f'{hour}.json'
        _save_file(file_name, response.text, f'data/{ execution_date }')


def _transform_pm_rates(data:json):
   """
   Divide one by the rate to get reverse for base symbol (price for one troy ounce).
   """
   pass


def _load_pm_rates():
   pass


filepath = """
data/{{ ds }}/{{ execution_date.hour }}.json
"""
check_file_operator = FileSensor(
    task_id='check_file',
    filepath=filepath,
    dag=dag
)

extract_pm_rates = PythonOperator(
    task_id='extract_pm_rates',
    python_callable=_extract_pm_rates,
    dag=dag,
    op_kwargs={'base': BASE_SYMBOL, 'symbols': SYMBOLS},
    trigger_rule='one_failed',
)
    

# if __name__ == "__main__":
#     _extract_pm_rates(BASE_SYMBOL, SYMBOLS, '12', '2023-10-14')


check_file_operator >> extract_pm_rates

"""
Target tasks:
IF API DATA EXISTS IN DATE/HOUR - DO NOT FETCH DATA
IF TRANSFORMED DATA EXISTS IN DATE/HOUR - DO NOT TRANSFORM DATA (???)

check_if_data_exists >> ((fetch_and_save_api_data >> bucket_sensor >> get_transform_save_data >> bucket_sensor) or ) 

"""