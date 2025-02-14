import asyncio
import os
import sys
from airflow import DAG
from airflow.operators.python import PythonOperator

sys.path.append(os.path.abspath("processing"))
sys.path.append(os.path.abspath("helper"))
from processing.load_fact_tables import process_fetch_tables
from processing.process_report_tele_bot import *
from processing.transfer_data import cut_off_data
from helper.config import Config
from helper.custom_logging import setup_logging

config = Config().get_config()

LOG_DIR = config["log"]["path"]
logger = setup_logging(LOG_DIR)


def run_fact_tables():
    # Chạy coroutine trong vòng lặp sự kiện
    asyncio.run(process_fetch_tables())


def run_process_signature_transaction():
    # Chạy coroutine trong vòng lặp sự kiện
    asyncio.run(job_accumulate_credential())


def run_process_accumulate_credential():
    # Chạy coroutine trong vòng lặp sự kiện
    asyncio.run(job_accumulate_credential())


def run_processing_cert_order_register():
    # Chạy coroutine trong vòng lặp sự kiện
    asyncio.run(job_cert_order_register())


def run_cut_off_signature_transaction():
    # Chạy coroutine trong vòng lặp sự kiện
    asyncio.run(cut_off_data("signature_transaction"))


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.now(),
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
    python_callable=run_fact_tables,
    dag=dag1
)

dag2 = DAG(
    'process_signature_transaction_by_month_app',
    default_args=default_args,
    schedule_interval='5 12 * 1 *',  # At 12:05 in January
    catchup=False
)

run_task2 = PythonOperator(
    task_id='process_signature_transaction_by_month_app',
    python_callable=run_process_signature_transaction,
    dag=dag2
)

dag3 = DAG(
    'process_accumulate_credential',
    default_args=default_args,
    schedule_interval='5 1 * * *',  # At 01:05 every day
    catchup=False
)

run_task3 = PythonOperator(
    task_id='process_accumulate_credential',
    python_callable=run_process_accumulate_credential,
    dag=dag3
)

dag4 = DAG(
    'processing_cert_order_register',
    default_args=default_args,
    schedule_interval='05 2 * * *',  # At 02:05 every day
    catchup=False
)

run_task4 = PythonOperator(
    task_id='processing_cert_order_register',
    python_callable=run_processing_cert_order_register,
    dag=dag3
)

dag5 = DAG(
    'cut_off_signature_transaction',
    default_args=default_args,
    schedule_interval='5 3 * * *',  # At 03:05 every day
    catchup=False
)

run_task5 = PythonOperator(
    task_id='cut_off_signature_transaction',
    python_callable=run_cut_off_signature_transaction,
    dag=dag3
)
