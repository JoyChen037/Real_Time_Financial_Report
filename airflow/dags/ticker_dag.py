from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta

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
    'ticker_dag',
    default_args=default_args,
    schedule_interval="0 7 * * *",  # Run every day at 7 AM
    description='Runs everyday to extract historical pricing data of S&P500 companies'
)

# Task to get the list of S&P500 companies using a BashOperator
ticker_t1 = BashOperator(
    task_id='get_sp500_comp_list',
    bash_command='./getSP500Comp.sh',  # Command to execute
    dag=dag  # Assign the DAG instance
)

# Task to complete the ticker price ETL process using another BashOperator
price_t2 = BashOperator(
    task_id='complete_ticker_price_etl',
    bash_command='./getStockHistPrice.sh',  # Command to execute
    dag=dag  # Assign the DAG instance
)

# Set task dependencies
ticker_t1 >> price_t2  # Make price_t2 depend on ticker_t1
