import logging
import requests

from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator

base_url = "http://77.37.136.11:7072/model_fast_api/v1"


def call_update_model_api():
    url = f"{base_url}/update_model"
    try:
        response = requests.post(url)
        response.raise_for_status()
        massage = response.json()
        logging.info(f'>>> SUCCESS {massage}')
    except requests.exceptions.HTTPError as http_err:
        logging.error(f"HTTP error occurred: {http_err}")
        raise http_err
    except Exception as err:
        logging.error(f"Other error occurred: {err}")
        raise err


with DAG(
        dag_id="update_model_api",
        start_date=datetime(2025, 2, 18),
        schedule_interval=timedelta(minutes=60),
        catchup=False,
) as dag:
    update_model_api_task = PythonOperator(
        task_id="update_model_api_task",
        python_callable=call_update_model_api
    )

    update_model_api_task