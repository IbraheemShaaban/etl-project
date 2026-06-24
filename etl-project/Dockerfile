FROM apache/airflow:2.8.0

USER airflow
RUN pip install --no-cache-dir azure-storage-blob requests

FROM apache/airflow:2.8.0 
RUN pip install apache-airflow-providers-snowflake