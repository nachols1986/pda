import os
import pandas as pd
import redshift_connector
from dotenv import dotenv_values
import awswrangler as wr

# Cargar las credenciales desde el archivo .env
path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
env_path = f'{path}/dags/env/redshift_key.env'
credentials = dotenv_values(env_path)

# Definir los parámetros de conexión
conn_params = {
    'host': 'redshift-pda-cluster.cnuimntownzt.us-east-2.redshift.amazonaws.com',
    'database': 'pda',
    'user': credentials['user'],
    'password': credentials['password'],
    'port': 5439,
}

def get_connection():
    """Establece y retorna la conexión a la base de datos Redshift."""
    return redshift_connector.connect(**conn_params)

def upload_to_redshift(file_name, table_name, mode, conn, schema):
    """Sube un archivo CSV a Redshift."""
    data_clean_dir = f'{os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))}/dags/data/clean/'
    df = pd.read_csv(f'{data_clean_dir}/{file_name}')
    wr.redshift.to_sql(
        df=df,
        con=conn,
        table=table_name,
        schema=schema,
        mode=mode,
        use_column_names=True,
        lock=True,
        index=False
    )

def upload_files_to_redshift():
    """Sube todos los archivos a las tablas correspondientes en Redshift."""
    conn = get_connection()
    schema = f"{conn_params['user']}_schema"

    files_to_tables = {
        'station_info_procesada.csv': 'stations_info',
        'station_status_procesada.csv': 'stations_status',
        'station_availability.csv': 'stations_availability',
        'station_free_bikes.csv': 'stations_free_bikes',
    }

    # Subir archivos a tablas
    upload_to_redshift('station_info_procesada.csv', 'stations_info', 'overwrite', conn, schema)
    for file_name, table_name in files_to_tables.items():
        if table_name != 'stations_info':
            upload_to_redshift(file_name, table_name, 'append', conn, schema)

    print("Todo subido a Redshift!! =)")
    print(schema)

if __name__ == '__main__':
    upload_files_to_redshift()
    
