from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email_operator import EmailOperator
from datetime import datetime
from airflow.decorators import dag, task

import sys
import os

dag_path = os.getenv('DAG_PATH')

sys.path.append(os.path.abspath(dag_path))

from etl import *

email = os.getenv('EMAIL')


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 4, 20),
    'email': [email],
    'email_on_failure': True,
    'email_on_retry': True,
    'email_on_success': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

@dag(
    default_args=default_args,
    description='An ETL workflow for integrating Grammy Awards data with Spotify datasets, performing transformations, merging, and loading the resulting data into Google Drive',
    schedule_interval='@daily',
)
def etl_workshop():

    
    @task
    def db_grammy_task():
        return grammy_process()
    
    @task
    def grammy_transformations_task(json_data):
        return transform_grammys_data(json_data)

    @task
    def read_spotify_data_task():
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
    def store_task(json_data):
        return load_dataset_to_drive(json_data, 'songs_data.csv', '1LxynhSi5b4IBvddJTey9RrTQDfk_Cq_b')

    def send_success_email():
        task_id = "send_success_email_task"
        subject = "The DAG has completed successfully!"
        current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        html_content = f"<p>The DAG completed without errors. Current time: {current_time}.</p>"
        to = email  
        return EmailOperator(
            task_id=task_id,
            to=to,
            subject=subject,
            html_content=html_content,
        )

    first = db_grammy_task()
    second = grammy_transformations_task(first)

    third = read_spotify_data_task()
    fourth = transform_spotify_data_task(third)

    fifth = merge_data_task(second, fourth)

    sixth = load_data_task(fifth)

    seventh = store_task(sixth)

    eighth = send_success_email()

    first >> second

    third >> fourth 
    
    fourth >> fifth >> sixth >> seventh >> eighth

    

workflow_api_etl_dag = etl_workshop()