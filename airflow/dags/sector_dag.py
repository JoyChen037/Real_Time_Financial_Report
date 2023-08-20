import yfinance as yf
import pandas as pd
from datetime import timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime


# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email': ['joychen20010303@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'catchup': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# Create the DAG instance
dag = DAG(
    'sector_dag',
    default_args=default_args,
    schedule_interval="0 7 * * *",  # Run every day at 7 AM
    description='Runs every day to extract historical pricing data of different sectors\' ETFs'
)

# Task to complete the sector ETF price ETL process
sector_t1 = BashOperator(
    task_id='complete_sector_etl',
    bash_command='./getSectorHistPrice.sh',  # Command to execute
    dag=dag  # Assign the DAG instance
)

# Display the task dependency graph
sector_t1