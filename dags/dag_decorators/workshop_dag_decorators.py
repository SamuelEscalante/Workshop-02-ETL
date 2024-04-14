from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from airflow.decorators import dag, task


import sys
import os

sys.path.append(os.path.abspath("/home/samuelescalante/prueba_workshop/dags/dag_decorators"))

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
    def grammy_task():
        return grammy_process()
    
    @task
    def grammy_transformations_task(json_data):
        return transform_grammys_data(json_data)

    @task
    def read_spotyfy_data_task():
        return read_spotify_data('data/spotify_dataset.csv')

    @task
    def transform_spotify_data_task(json_data):
        return transform_spotify_data(json_data)
    
    @task
    def merge_data_task(json_data1, json_data2):
        return merge_datasets(json_data1, json_data2)
    
    @task
    def load_data_task(json_data):
        return load_merge(json_data)
    
    @task
    def store(json_data):
        return load_dataset_to_drive(json_data, 'songs_data.csv', '1LxynhSi5b4IBvddJTey9RrTQDfk_Cq_b')

    primera = grammy_task()
    segunda = grammy_transformations_task(primera)

    tercera = read_spotyfy_data_task()
    cuarta = transform_spotify_data_task(tercera)

    quinta = merge_data_task(segunda, cuarta)
    
    sexta = load_data_task(quinta)

    septima = store(sexta)
    

workflow_api_etl_dag = etl_workshop()