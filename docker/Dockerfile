FROM apache/airflow:2.7.3

USER root


# Создаем директорию для DAG'ов
RUN mkdir -p /opt/airflow/dags

# Копируем DAG'и в контейнер (если у вас есть)
COPY src/dags/ /opt/airflow/dags/

EXPOSE 8502

USER airflow
