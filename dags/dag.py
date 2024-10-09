from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import timedelta, datetime
import os

from src.conexion_api import download_station_data
from src.process_ecobici_data import process_ecobici_data
from src.create_aggregated_tables import process_station_data
from src.upload_to_redshift import upload_files_to_redshift

path = os.environ['AIRFLOW_HOME']

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 10, 8),
    'schedule_interval': '0 9 * * *', 
}

dag = DAG(
    dag_id='ecobici_dag',
    description='ETL para procesar datas de Ecobici Bs As',
    default_args=default_args,
    catchup=True,
    max_active_tasks=32
)

# Definir las tareas usando las funciones importadas
task1 = PythonOperator(
    task_id='download_station_data',
    python_callable=download_station_data,
    dag=dag
)

task2 = PythonOperator(
    task_id='process_ecobici_data',
    python_callable=process_ecobici_data,
    dag=dag
)

task3 = PythonOperator(
    task_id='process_station_data',
    python_callable=process_station_data,
    dag=dag
)

task4 = PythonOperator(
    task_id='upload_files_to_redshift',
    python_callable=upload_files_to_redshift,
    dag=dag
)

# Definir las dependencias
task1 >> task2 >> task3 >> task4
