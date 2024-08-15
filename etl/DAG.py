from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import sys
from google.cloud import storage
import os

# Add etl package to the Python path
sys.path.insert(0, '/home/airflow/gcs/dags')

from etl.extraction import extract_and_upload_data
from transformation import transform_data
from etl.loading import load_data_to_bigquery

# Default arguments
default_args = {
    'owner': 'dartech',
    'depends_on_past': False,
    'start_date': datetime(2024, 7, 30),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': None
    #'retry_delay': timedelta(minutes=1),
    
}

# Define the DAG
dag = DAG(
    'nyc_payroll_etl_dag',
    default_args=default_args,
    description='ETL pipeline for NYC Payroll Data',
    
)

# Define the tasks
extract_task = PythonOperator(
    task_id='fetch_data',
    python_callable=extract_and_upload_data,
    op_args=['/home/airflow/gcs/dags/service_account_key.json'],
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    op_args=['/home/airflow/gcs/dags/service_account_key.json'],
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_data',
    python_callable=load_data_to_bigquery,
    op_args=['/home/airflow/gcs/dags/service_account_key.json'],
    dag=dag,
)

# Set task dependencies
extract_task >> transform_task >> load_task
