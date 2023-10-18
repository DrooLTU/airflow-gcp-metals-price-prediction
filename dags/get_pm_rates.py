import os
from dotenv import load_dotenv

from airflow import DAG, Dataset
from airflow.models import Variable
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.sensors.filesystem import FileSensor
from airflow.exceptions import AirflowException


from datetime import datetime, timedelta
from typing import List
import requests
import json

from pyarrow import json as pa_json
import pyarrow.parquet as pq


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

TRANSFORMED_DATA = Dataset(f'file://opt/airflow/data/datasets/transformed_pm_rates.parquet')

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
    schedule='@hourly',
    catchup=False,
)


def _save_parquet(json_path:str, filename:str, dir:str):
    """
    Saves data as a Parquet file.

    Args:
      json_path: Path to JSON file to read data from.
      filename: The name of the file to save.
      dir: The directory to save the file to.
    """

    table = pa_json.read_json(json_path) 
    pq.write_table(table, f'{dir}{filename}')


def _save_file(filename:str, file_content:str, dir:str):
    """
    Saves a file to the specified directory.

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


def _check_extracted_does_not_exist(filename: str, dir: str ='') -> bool:
    """
    Checks if extracted data for the date/hour exists in case of previous
    downstream task failure.

    Args:
      filename: The name of the file to check for.
      dir: The directory of the file.
    """
    file_path = os.path.join(dir, filename)
    print(file_path)

    if os.path.isfile(file_path):
        return "transform_existing_pm_rates"
    
    return "extract_pm_rates"


def _extract_pm_rates(base: str, symbols: List[str]) -> None:
    """
    Makes the API call to get the data and saves it as JSON on file system.

    Args:
      base: base symbol for the prices (USD).
      symbols: symbol list to get the prices for (XAU, EUR, XPT, ...).
    """
    session = requests.Session()
    symbols_str = ",".join(symbols)
    url = f"{base_url}?api_key={API_KEY}&base={base}&currencies={symbols_str}"
    response = session.get(url)
    print(response)
    if response.status_code == 200:
        
        json_data = json.loads(response.text)
        datetime_object = datetime.fromtimestamp(json_data['timestamp'])
        file_name = f"{datetime_object.strftime('%Y-%m-%d-%H')}.json"

        _save_file(file_name, response.text, "/opt/airflow/data/extracted")
    
    else:
       raise AirflowException(f"Invalid response code, expected 200, got {response.status_code}") 


def _transform_pm_rates(filename: str):
    """
    Reads the extracted data file (JSON) and does data wrangling.
    Divide one by the rate to get price for one troy ounce for the base symbol.
    Saves transformed data to file system and as Parquet dataset for further processing.

    Args:
      filename: The name of the data file.
    """
    if filename.endswith('.json'):
        with open(filename, "r") as f:
            json_data = json.load(f)
            rates = json_data['rates']
            timestamp = json_data['timestamp']
            datetime_object = datetime.fromtimestamp(timestamp)
            transformed_data = {'data_datetime': datetime_object.isoformat()}
            base = json_data['base']
            
            print(rates)
            for rate, val in rates.items():
                transformed_data[f'{rate}{base}'] = 1 / val
                print(f'rate: {rate}, val: {val}')

            file_name = f"{datetime_object.strftime('%Y-%m-%d-%H')}.json"
            _save_file(file_name, json.dumps(transformed_data), "/opt/airflow/data/transformed")

            _save_parquet(f'/opt/airflow/data/transformed/{file_name}', 'transformed_pm_rates.parquet', '/opt/airflow/data/datasets/')
    
    else:
        raise AirflowException(f"Unsupported file format, expected JSON, got {filename.split('.')[-1]}")


adjusted_dth = datetime.now() - timedelta(hours=1)
adjusted_dth_str = adjusted_dth.strftime('%Y-%m-%d-%H')
filepath = f'/opt/airflow/data/extracted/{adjusted_dth_str}.json'
filepath_transformed = f'/opt/airflow/data/transformed/{adjusted_dth_str}.json'


extracted_data_does_not_exist = BranchPythonOperator(
    task_id="extracted_data_does_not_exist",
    python_callable=_check_extracted_does_not_exist,
    dag=dag,
    op_kwargs={"filename": filepath},
)

extract_pm_rates = PythonOperator(
    task_id="extract_pm_rates",
    python_callable=_extract_pm_rates,
    dag=dag,
    op_kwargs={"base": BASE_SYMBOL, "symbols": SYMBOLS},
)

sense_extracted_file = FileSensor(
    task_id="sense_extracted_file",
    filepath=filepath,
    timeout=10,
    mode="reschedule",
    poke_interval=10,
    dag=dag,
)

transform_existing_pm_rates = PythonOperator(
    task_id="transform_existing_pm_rates",
    python_callable=_transform_pm_rates,
    dag=dag,
    op_kwargs={"filename": filepath},
    outlets=[TRANSFORMED_DATA]
)

transform_new_pm_rates = PythonOperator(
    task_id="transform_new_pm_rates",
    python_callable=_transform_pm_rates,
    dag=dag,
    op_kwargs={"filename": filepath},
    outlets=[TRANSFORMED_DATA]
)


extracted_data_does_not_exist >> [extract_pm_rates, transform_existing_pm_rates]

extract_pm_rates >> sense_extracted_file >> transform_new_pm_rates
