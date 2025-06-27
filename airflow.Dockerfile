FROM apache/airflow:2.5.1

USER airflow
RUN pip install pydantic==1.10.13 typing-extensions==4.5.0 clickhouse_driver