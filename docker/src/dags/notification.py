import logging
import requests

from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator

base_url = "http://77.37.136.11:7080/alert_manager/v1/notification"


def send_notification():
    url = base_url
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
        dag_id="send_notification_dag",
        start_date=datetime(2025, 2, 18),
        schedule_interval=timedelta(minutes=15),
        catchup=False,
) as dag:
    send_notification_task = PythonOperator(
        task_id="send_notification_task",
        python_callable=send_notification
    )

    send_notification_task