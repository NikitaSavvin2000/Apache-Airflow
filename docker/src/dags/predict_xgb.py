import logging
import requests

from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator

base_url = "http://77.37.136.11:7072/model_fast_api/v1"


def call_predict_xgb_api(count_time_points_predict: int = 288):
    url = f"{base_url}/predict_xgb"
    payload = {"count_time_points_predict": count_time_points_predict}
    try:
        response = requests.post(url, json=payload)
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
        dag_id="predict_xgb_api_dag",
        start_date=datetime(2025, 2, 18),
        schedule_interval=timedelta(minutes=5),
        catchup=False,
) as dag:
    predict_xgb_task = PythonOperator(
        task_id="call_predict_xgb_api_task",
        python_callable=call_predict_xgb_api
    )

    predict_xgb_task