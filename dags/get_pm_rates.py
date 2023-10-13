import os
import tempfile
from dotenv import load_dotenv
from airflow import DAG
from airflow.models import Variable
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

  temp_file_path = tempfile.NamedTemporaryFile(dir=dir_path, delete=False)

  with open(temp_file_path, "w") as f:
    f.write(file_content)

  try:
    os.replace(temp_file_path.name, os.path.join(dir_path, file_name))
  except:
    os.remove(temp_file_path.name)


def _get_pm_rates(base:str, symbols:List[str]) -> None:
    session = requests.Session()
    symbols_str = ','.join(symbols)
    url = f'{base_url}?api_key={API_KEY}&base={base}&currencies={symbols_str}'
    response = session.get(url)
    print(response)
    if response.status_code == 200:
        json_data = json.loads(response.text)
        print(json_data["timestamp"])
        file_name = f'{json_data["timestamp"]}.json'
        _save_file(file_name, response.text, 'data')
    

if __name__ == "__main__":
    _get_pm_rates(BASE_SYMBOL, SYMBOLS)