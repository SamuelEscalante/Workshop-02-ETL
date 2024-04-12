import os 
import sys
sys.path.append(os.path.abspath("/opt/airflow/dags/workshop_dag/"))

from datetime import timedelta
from airflow import DAG
from datetime import datetime

from airflow.operators.python import PythonOperator

from merge import *

def load():
    print("loading...")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 4, 11),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    'workshop_dag',
    default_args=default_args,
    description='A simple workshop DAG',
    schedule_interval='@daily',
) as dag:
    
    prueba = PythonOperator(
        task_id='load',
        python_callable=load,
        dag=dag,
    )

    read_csv_task = PythonOperator(
        task_id='read_csv',
        python_callable=read_spotify_data,
        op_kwargs={'file_path' : 'data/spotify_dataset.csv'},
        dag=dag,
    )

    prueba >> read_csv_task



    

    