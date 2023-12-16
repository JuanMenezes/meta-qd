FROM apache/airflow:2.7.3
USER airflow

RUN pip install pandas

USER airflow
