import os
from dotenv import load_dotenv

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.sensors.filesystem import FileSensor
from airflow.exceptions import AirflowFailException

from datetime import datetime, timedelta
from typing import List
import requests
import json


load_dotenv()


default_args = {
    "owner": "Justinas",
    "depends_on_past": False,
    "start_date": datetime(2023, 10, 13),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

BASE_SYMBOL = "USD"
SYMBOLS = ["EUR", "XAU", "XAG", "XPD", "XPT"]

API_KEY = os.getenv("API_KEY")
if not API_KEY:
    API_KEY = Variable.get("pm_api_key")


base_url = "https://api.metalpriceapi.com/v1/latest"

dag = DAG(
    "get_pm_rates",
    default_args=default_args,
    description="Get and store the precious metal rates from the API",
    schedule=None,
    catchup=False,
)


def _save_file(filename, file_content, dir):
    """Saves a file to the specified directory.

    Args:
      filename: The name of the file to save.
      file_content: The content of the file to save.
      dir: The directory to save the file to.
    """

    if not os.path.exists(dir):
        os.makedirs(dir)

    file_path = os.path.join(dir, filename)

    with open(file_path, "w") as f:
        f.write(file_content)


def _check_file_does_not_exist(filename: str, dir: str ='') -> bool:
    file_path = os.path.join(dir, filename)
    print(file_path)

    if os.path.isfile(file_path):
        return "transform_pm_rates"
    
    return "extract_pm_rates"


def _extract_pm_rates(base: str, symbols: List[str], **kwargs) -> None:
    session = requests.Session()
    symbols_str = ",".join(symbols)
    url = f"{base_url}?api_key={API_KEY}&base={base}&currencies={symbols_str}"
    response = session.get(url)
    print(response)
    if response.status_code == 200:
        
        json_data = json.loads(response.text)
        print(json_data)
        datetime_object = datetime.fromtimestamp(json_data['timestamp'])
        file_name = f"{datetime_object.strftime('%Y-%m-%d-%H')}.json"

        _save_file(file_name, response.text, "/opt/airflow/data/")


def _transform_pm_rates(filename: str):
    """
    Divide one by the rate to get reverse for base symbol (price for one troy ounce).
    """
    with open(filename, "r") as f:
        json_data = json.load(f)
        rates = json_data['rates']
        for rate in rates:
            print(rate)


def _load_pm_rates():
    pass


#THIS IS NEEDE FOR NOW TO COMPENSATE FOR TIME DIFF, NEED BETTER SOLUTION
adjusted_dth = datetime.now() - timedelta(hours=2)
adjusted_dth_str = adjusted_dth.strftime('%Y-%m-%d-%H')
filepath = f'/opt/airflow/data/{adjusted_dth_str}.json'

extracted_data_does_not_exist = BranchPythonOperator(
    task_id="extracted_data_does_not_exist",
    python_callable=_check_file_does_not_exist,
    dag=dag,
    op_kwargs={"filename": filepath},
)

extract_pm_rates = PythonOperator(
    task_id="extract_pm_rates",
    python_callable=_extract_pm_rates,
    dag=dag,
    op_kwargs={"base": BASE_SYMBOL, "symbols": SYMBOLS},
)

transform_pm_rates = PythonOperator(
    task_id="transform_pm_rates",
    python_callable=_transform_pm_rates,
    dag=dag,
    op_kwargs={"filename": filepath},
)


extracted_data_does_not_exist >> [extract_pm_rates, transform_pm_rates]



"""
Target tasks:
IF API DATA EXISTS IN DATE/HOUR - DO NOT FETCH DATA
IF TRANSFORMED DATA EXISTS IN DATE/HOUR - DO NOT TRANSFORM DATA (???)

check_if_data_exists >> ((fetch_and_save_api_data >> bucket_sensor >> get_transform_save_data >> bucket_sensor) or ) 

"""
