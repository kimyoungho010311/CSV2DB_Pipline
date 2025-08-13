FROM apache/airflow:3.0.4
USER airflow
RUN pip install apache-airflow-providers-postgres
USER airflow