from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import timedelta, datetime
import os

path = os.environ['AIRFLOW_HOME']

default_args = {
                'owner': 'airflow',
                'depends_on_past': False,
                'email_on_failure': False,
                'email_on_retry': False,
                'retries': 1,
                'retry_delay': timedelta(minutes=5),
                'start_date': datetime(2024, 9, 23),
                'schedule_interval': '0 9 * * *', 
                }

dag = DAG(
            dag_id='ecobici_dag',
            description='ETL para procesar datas de Ecobici Bs As',
            default_args=default_args,
            catchup=True,
            max_active_tasks=32
            )

def conexion_api():
    os.system(f'python {path}/dags/src/conexion_api.py')

def process_ecobici_data():
    os.system(f'python {path}/dags/src/process_ecobici_data.py')

def create_aggregated_tables():
    os.system(f'python {path}/dags/src/create_aggregated_tables.py')

def upload_to_redshift():
    os.system(f'python {path}/dags/src/upload_to_redshift.py')

task1 = PythonOperator(
    task_id='conexion_api',
    python_callable=conexion_api,
    dag=dag
)

task2 = PythonOperator(
    task_id='process_ecobici_data',
    python_callable=process_ecobici_data,
    dag=dag
)

task3 = PythonOperator(
    task_id='create_aggregated_tables',
    python_callable=create_aggregated_tables,
    dag=dag
)

task4 = PythonOperator(
    task_id='upload_to_redshift',
    python_callable=upload_to_redshift,
    dag=dag
)


task1 >> task2 >> task3 >> task4