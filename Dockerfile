FROM apache/airflow:2.8.4-python3.9
ADD requirements.txt .
RUN pip install apache-airflow==${AIRFLOW_VERSION} -r requirements.txt