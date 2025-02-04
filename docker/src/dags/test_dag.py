from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import random
import logging

log = logging.getLogger(__name__)

counter = 0


def print_hello():
    global counter
    counter += 1
    messages = [
        "Airflow DAG запущен!",
        "Еще один запуск DAG-а!",
        "DAG выполняется корректно!",
        "Очередной запуск, все работает!",
        "Тестовый DAG продолжает работать!"
    ]
    message = f"#{counter}: {random.choice(messages)}"
    log.info("=" * len(message))
    log.info(message)
    log.info("=" * len(message))


def print_goodbye():
    log.info("DAG успешно завершен! Всё прошло отлично.")

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
        dag_id='test_dag',
        default_args=default_args,
        description='DAG с двумя последовательными задачами',
        schedule_interval='* * * * *',
        start_date=datetime(2024, 1, 1),
        catchup=False
) as dag:

    task_hello = PythonOperator(
        task_id='print_hello',
        python_callable=print_hello
    )

    task_goodbye = PythonOperator(
        task_id='print_goodbye',
        python_callable=print_goodbye
    )

    task_hello >> task_goodbye
