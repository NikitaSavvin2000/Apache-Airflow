FROM apache/airflow:2.7.3

USER root

RUN apt-get update && apt-get install -y \
    build-essential \
    libhdf5-dev \
    python3-dev \
    libatlas-base-dev \
    liblapack-dev \
    gfortran \
    libffi-dev

RUN pip install tensorflow
RUN pip install h5py

RUN mkdir -p /opt/airflow/dags

COPY src/dags/ /opt/airflow/dags/

USER airflow

EXPOSE 8080

