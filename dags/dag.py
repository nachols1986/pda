from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import timedelta, datetime
import os

from src.conexion_api import download_station_data
from src.process_ecobici_data import process_ecobici_data
from src.create_aggregated_tables import process_station_data
from src.upload_to_redshift import upload_files_to_redshift
from src.send_mails import ejecutar_tareas_mailing

path = os.environ['AIRFLOW_HOME']

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 10, 8),
}

dag = DAG(
    dag_id='ecobici_dag',
    description='ETL para procesar datas de Ecobici Bs As',
    default_args=default_args,
    schedule_interval='@hourly',
    catchup=False,
    max_active_tasks=32
)

task1 = PythonOperator(
    task_id='download_station_data',
    python_callable=download_station_data,
    execution_timeout=timedelta(minutes=2),  # Timeout de 2 minutos (más no debería tardar)
    dag=dag
)

task2 = PythonOperator(
    task_id='process_ecobici_data',
    python_callable=process_ecobici_data,
    trigger_rule='all_success', 
    dag=dag
)

task3 = PythonOperator(
    task_id='process_station_data',
    python_callable=process_station_data,
    trigger_rule='all_success',
    dag=dag
)

task4 = PythonOperator(
    task_id='upload_files_to_redshift',
    python_callable=upload_files_to_redshift,
    trigger_rule='all_success',
    dag=dag
)

task5 = PythonOperator(
    task_id='send_mails',
    python_callable=ejecutar_tareas_mailing,
    trigger_rule='all_success',
    dag=dag
)

task1 >> task2 >> task3 >> [task4, task5]
