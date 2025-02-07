from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import random
import logging
from airflow.decorators import task
from airflow.models import Variable
from airflow.providers.ftp.hooks.ftp import FTPHook


log = logging.getLogger(__name__)

counter = 0

"""
Пример вызова переменной из airflow
Переменная с ключем SOME_KEY задается в http://localhost:8080/home -> Admin -> Variables
"""
some_key = Variable.get("SOME_KEY", default_var='test')


"""
Пример создания хука из airflow (Хук - подключение через аирфлоу к чему-либо) в данном примере FTPHook бывают другие виды
Соеденение с ключем test_ftp_conn_id задается в http://localhost:8080/home -> Admin -> Connections
"""
ftp_conn_id = "test_ftp_conn_id"
ftp_hook = FTPHook(ftp_conn_id)
ftp_hook.secure = False


@task.virtualenv(
    task_id="virtualenv_python", requirements=["minio>=7.2.15"], system_site_packages=False
)
def print_hello():
    """
    @task.virtualenv как пример создания виртуального окружения для функции, как пример взять minio>=7.2.15
    """
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
