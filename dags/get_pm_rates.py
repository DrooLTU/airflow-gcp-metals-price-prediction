import os 
from dotenv import load_dotenv
from airflow import DAG
from airflow.models import Variable
from datetime import datetime, timedelta
from typing import List
import requests

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
API_KEY = Variable.get("pm_api_key")

if not API_KEY:
    API_KEY = os.getenv("API_KEY")


base_url = 'https://api.metalpriceapi.com/v1/latest'

dag = DAG(
    'get_pm_rates',
    default_args=default_args,
    description='Get and store the precious metal rates from the API',
    schedule_interval=None,
    catchup=False,
)


def _get_pm_rates(base:str, symbols:List[str]):
    session = requests.Session()
    symbols_str = ','.join(SYMBOLS)
    url = f'{base_url}?api_key={API_KEY}&base={BASE_SYMBOL}&currencies={symbols_str}'
    response = session.get(url)
    print(response)
    if response.status_code == 200:
        return response.text
    

if __name__ == "__main__":
    _get_pm_rates(BASE_SYMBOL, SYMBOLS)