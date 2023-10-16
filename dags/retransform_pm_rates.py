import os

from airflow import DAG, Dataset
from airflow.operators.python import PythonOperator

from datetime import datetime, timedelta
from typing import List
import json

import pyarrow as pa
from pyarrow import json as pa_json
import pyarrow.parquet as pq

default_args = {
    "owner": "Justinas",
    "depends_on_past": False,
    "start_date": datetime(2023, 10, 13),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}

TRANSFORMED_DATA = Dataset(f'file://opt/airflow/data/datasets/transformed_pm_rates.parquet')

dag = DAG(
    "retransform_pm_rates",
    default_args=default_args,
    description="THIS IS USED WHEN TRANSORMATION IS CHANGED, WORKS ONLY ON EXISTING EXTRACTED DATA. RUN THIS ONLY WHEN NECESSARY.",
    schedule=None,
    catchup=False,
)


def _save_parquet(json_path: str, filename: str, dir: str):
    if json_path.endswith('.json'):
        table = pa_json.read_json(json_path)
        output_path = os.path.join(dir, filename + '.parquet')
        pq.write_table(table, output_path)

    else:
        aggregated_table = None
        for file in os.listdir(json_path):
            if file.endswith('.json'):
                table = pa_json.read_json(os.path.join(json_path, file))
                if aggregated_table is None:
                    aggregated_table = table
                else:
                    aggregated_table = pa.concat_tables([aggregated_table, table])

        output_path = os.path.join(dir, filename + '.parquet')
        pq.write_table(aggregated_table, output_path)


def _save_file(filename:str, file_content:str, dir:str):
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


def _transform_pm_rates(file_path: str):
    """
    Divide one by the rate to get reverse for base symbol (price for one troy ounce).
    """
    with open(file_path, "r") as f:
        json_data = json.load(f)
        rates = json_data['rates']
        timestamp = json_data['timestamp']
        datetime_object = datetime.fromtimestamp(timestamp)
        transformed_data = {'data_datetime': datetime_object.isoformat()}
        base = json_data['base']

        for rate, val in rates.items():
            transformed_data[f'{rate}{base}'] = 1 / val
            print(f'rate: {rate}, val: {val}')

        #BACKUP STORE
        file_name = f"{datetime_object.strftime('%Y-%m-%d-%H')}.json"
        _save_file(file_name, json.dumps(transformed_data), "/opt/airflow/data/transformed/")


def create_transform_task(dag, file_path):
    task_id = f'transform_{os.path.splitext(os.path.basename(file_path))[0]}'
    return PythonOperator(
        task_id=task_id,
        python_callable=_transform_pm_rates,
        op_args=[file_path],
        dag=dag,
    )

aggregate_task = PythonOperator(
    task_id='aggregate_parquet_files',
    python_callable=_save_parquet,
    op_args=['/opt/airflow/data/transformed/', 'transformed_pm_rates', '/opt/airflow/data/datasets/'],
    dag=dag,
    outlets=[TRANSFORMED_DATA]
)

input_dir= "/opt/airflow/data/extracted/"

for file in os.listdir(input_dir):
    if file.endswith('.json'):
        file_path = os.path.join(input_dir, file)
        transform_task = create_transform_task(dag, file_path)
        transform_task >> aggregate_task

if __name__ == "__main__":
    dag.cli()