from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from airflow.decorators import dag, task
import os
import sys
sys.path.append(os.path.abspath("/opt/airflow/dags/dag_decorators/"))

from etl import *


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 4, 11),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

@dag(
    default_args=default_args,
    description='Our first DAG with ETL process!',
    schedule_interval='@daily',
)
def etl_workshop():

    @task
    def connect_to_database_task():
        return connect_to_database()

    @task
    def read_spotyfy_data_task():
        return read_spotify_data('data/spotify_dataset.csv')

    @task
    def transform_spotify_data_task(json_data):
        return transform_spotify_data(json_data)

    connect = connect_to_database_task()
    read = read_spotyfy_data_task()
    transform_spotify_data_task(read)

workflow_api_etl_dag = etl_workshop()