FROM apache/airflow:2.7.3

USER root

# Устанавливаем необходимые Python-библиотеки (если нужны)
RUN pip install --no-cache-dir requests pandas

# Создаем директорию для DAG'ов
RUN mkdir -p /opt/airflow/dags

# Копируем DAG'и в контейнер (если у вас есть)
COPY dags/ /opt/airflow/dags/

USER airflow
