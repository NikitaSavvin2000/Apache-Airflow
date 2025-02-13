import logging
import requests
import psycopg2
import numpy as np
import pandas as pd

from airflow import DAG
from airflow.models import Variable
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from airflow.operators.python import PythonOperator


start_write_data_date = '2024-01-16'

BATCH_SIZE = int(Variable.get("BATCH_SIZE", default_var=100))

dbname = Variable.get("dbname", default_var="pass")
user = Variable.get("user", default_var="pass")
password = Variable.get("password", default_var="pass")
host = Variable.get("host", default_var="pass")
port = int(Variable.get("port", default_var="pass"))

token = Variable.get("token", default_var="pass")
static_link = Variable.get("static_link", default_var="pass")

run_interval = int(Variable.get("run_interval", default_var=5))

DB_PARAMS = {
    "dbname": dbname,
    "user": user,
    "password": password,
    "host": host,
    "port": port
}


class Read:
    def __init__(self, token, static_link, read_interval):
        self.token = token
        self.static_link = static_link
        self.read_interval = read_interval

    def read_data_api(self, sensor_id, last_date, name_sensor):
        name_sensor = self.name_to_format(name_sensor)
        str_date = last_date.strftime("%Y%m%d") + "/"
        api_link = f"{self.static_link}{self.read_interval}/detailed/{str_date}{sensor_id}{self.token}"

        response = requests.get(api_link)
        logging.info(f"API request to {api_link}, status: {response.status_code}")

        if response.status_code != 200:
            logging.error(f"Failed to fetch data: {response.text}")
            return pd.DataFrame(columns=["Timestamp", name_sensor])

        df = pd.read_csv(api_link, sep=';')
        df = df.rename(columns={'Value1': name_sensor})
        return df

    def general_period(self, name_sensor, sensor_id, last_date):
        name_sensor = self.name_to_format(name_sensor)
        df_general_period = pd.DataFrame(columns=["Timestamp", name_sensor])
        df_general_period['Timestamp'] = pd.to_datetime(df_general_period['Timestamp'])
        df_general_period[name_sensor] = pd.to_numeric(df_general_period[name_sensor], errors='coerce')

        last_date = last_date.replace(tzinfo=None)
        today_date = datetime.today()

        while today_date > last_date:
            df_sensor = self.read_data_api(sensor_id, last_date, name_sensor)
            if df_sensor.empty:
                last_date += relativedelta(months=1)
                continue
            df_sensor = df_sensor.iloc[:-1]
            df_general_period = pd.concat([df_general_period, df_sensor], ignore_index=True)
            last_date += relativedelta(months=1)

        start_of_month = today_date.replace(day=1)
        df_sensor = self.read_data_api(sensor_id, start_of_month, name_sensor)
        df_sensor = df_sensor.iloc[:-1]
        df_general_period = pd.concat([df_general_period, df_sensor], ignore_index=True)

        df_general_period = df_general_period.rename(columns={'Timestamp': 'datetime'})
        df_general_period = df_general_period[['datetime', name_sensor]]
        df_general_period = df_general_period.drop_duplicates(subset=['datetime'], keep='first')

        return name_sensor, df_general_period

    def name_to_format(self, name):
        name = ''.join(['_' if not c.isalnum() else c for c in name]).rstrip('_')
        name = '_'.join(filter(None, name.split('_'))).lower()
        return name


def get_last_datetime(ti):
    try:
        conn = psycopg2.connect(**DB_PARAMS)
        cur = conn.cursor()
        cur.execute("SELECT MAX(datetime) FROM load_consumption;")
        last_datetime = cur.fetchone()[0]
        cur.close()
        conn.close()

        ti.xcom_push(key='last_datetime', value=last_datetime)
        logging.info(f"Last known datetime: {last_datetime}")
    except Exception as e:
        logging.error(f"Error fetching last datetime: {e}")
        ti.xcom_push(key='last_datetime', value=None)


def fetch_sensor_data(ti):
    last_date = ti.xcom_pull(task_ids='fetch_last_known_date', key='last_datetime')

    if last_date is None:
        last_date = datetime.strptime(start_write_data_date, '%Y-%m-%d')

    data = Read(
        token=token,
        static_link=static_link,
        read_interval='month'
    )

    df_data_sensor = data.general_period(
        name_sensor="load_consumption", sensor_id='arithmetic_1464947681', last_date=last_date)
    ti.xcom_push(key='df_data_sensor', value=df_data_sensor)

    logging.info(df_data_sensor)


def prepare_data_to_upload(ti):
    last_known_date = ti.xcom_pull(task_ids='fetch_last_known_date', key='last_datetime')
    last_known_date = last_known_date.replace(tzinfo=None)
    last_known_date = np.datetime64(last_known_date)

    df_data_sensor = ti.xcom_pull(task_ids='fetch_sensor_data', key='df_data_sensor')

    if isinstance(df_data_sensor, tuple) and len(df_data_sensor) == 2:
        df_data_sensor = df_data_sensor[1]

    if last_known_date is None:
        ti.xcom_push(key='df_to_upload', value=df_data_sensor)
        logging.info(f'Подготовили данные к загрузке')
        return

    df_data_sensor['datetime'] = pd.to_datetime(df_data_sensor['datetime'])
    df_data_sensor['datetime'] = df_data_sensor['datetime'].astype('datetime64[ns]')

    df_to_upload = df_data_sensor[df_data_sensor['datetime'] > last_known_date]
    ti.xcom_push(key='df_to_upload', value=df_to_upload)
    logging.info(f'Подготовлено {len(df_to_upload)} строк к загрузке')


def upload_to_db(ti):
    df_to_upload = ti.xcom_pull(task_ids='prepare_data_to_upload', key='df_to_upload')

    if isinstance(df_to_upload, tuple) and len(df_to_upload) == 2:
        df_to_upload = df_to_upload[1]

    if df_to_upload is None or df_to_upload.empty:
        logging.info("Нет данных для загрузки.")
        return

    df_to_upload = df_to_upload.dropna()
    total_rows = len(df_to_upload)

    logging.info(f'Загружаем {total_rows} строк в DB батчами по {BATCH_SIZE}...')

    try:
        conn = psycopg2.connect(**DB_PARAMS)
        cur = conn.cursor()

        insert_query = """
        INSERT INTO load_consumption (datetime, load_consumption)
        VALUES (%s, %s)
        ON CONFLICT (datetime) DO UPDATE 
        SET load_consumption = EXCLUDED.load_consumption;
        """

        for i, chunk in enumerate(np.array_split(df_to_upload, np.ceil(total_rows / BATCH_SIZE))):
            data_to_insert = [tuple(row) for row in chunk.itertuples(index=False, name=None)]
            cur.executemany(insert_query, data_to_insert)
            conn.commit()

            percent_complete = round(((i + 1) * BATCH_SIZE / total_rows) * 100, 2)
            logging.info(f"Загружено {min((i + 1) * BATCH_SIZE, total_rows)} / {total_rows} записей ({percent_complete}%).")

        cur.close()
        conn.close()

        logging.info(f"Загрузка завершена: {total_rows} записей загружено в таблицу load_consumption.")

    except Exception as e:
        logging.error(f"Ошибка при загрузке данных: {e}")
        raise  # Чтобы Airflow пометил DAG как упавший


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 2, 11),
    'retries': 1,
    'retry_delay': timedelta(minutes=run_interval),
}

dag = DAG(
    'sensor_data_fetcher',
    default_args=default_args,
    description='Fetch sensor data every 5 minutes',
    schedule_interval=timedelta(minutes=run_interval),
    catchup=False
)

fetch_last_known_date = PythonOperator(
    task_id='fetch_last_known_date',
    python_callable=get_last_datetime,
    provide_context=True,
    dag=dag
)

fetch_task = PythonOperator(
    task_id='fetch_sensor_data',
    python_callable=fetch_sensor_data,
    provide_context=True,
    dag=dag
)

prepare_data_to_upload = PythonOperator(
    task_id='prepare_data_to_upload',
    python_callable=prepare_data_to_upload,
    provide_context=True,
    dag=dag
)

upload_to_db = PythonOperator(
    task_id='upload_to_db',
    python_callable=upload_to_db,
    provide_context=True,
    dag=dag
)

fetch_last_known_date >> fetch_task >> prepare_data_to_upload >> upload_to_db