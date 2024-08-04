FROM apache/airflow:2.9.3-python3.12
USER airflow

RUN pip install pandas

USER airflow
