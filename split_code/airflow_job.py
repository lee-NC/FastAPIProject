import asyncio
import os
import sys
from datetime import timedelta

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator

sys.path.append(os.path.abspath("processing"))
sys.path.append(os.path.abspath("helper"))
from processing.load_fact_tables import process_fetch_tables
from helper.config import Config
from helper.custom_logging import setup_logging

config = Config().get_config()

LOG_DIR = config["log"]["path"]
logger = setup_logging(LOG_DIR)


def run_fact_tables():
    # Chạy coroutine trong vòng lặp sự kiện
    asyncio.run(process_fetch_tables())


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag1 = DAG(
    'fetch_mongo_to_hbase',
    default_args=default_args,
    schedule_interval='5 0 * * *',  # At 00:05 every day
    catchup=False
)

run_task1 = PythonOperator(
    task_id='process_signature_transaction_by_month_app',
    python_callable=run_fact_tables,  # Gọi hàm run_fact_tables thay vì gọi process_fetch_tables trực tiếp
    dag=dag1
)
