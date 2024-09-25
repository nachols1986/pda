import os
import pandas as pd
import redshift_connector
import awswrangler as wr
from dotenv import dotenv_values

# Verifica si AIRFLOW_HOME está definido, si no usa un path local
if 'AIRFLOW_HOME' in os.environ:
    path = os.environ['AIRFLOW_HOME']
else:
    # path = ./ecobici/
    path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))

data_clean_dir = f'{path}/dags/data/clean/'
env_path = f'{path}/dags/env/redshift_key.env'

# Leer credenciales
credentials = dotenv_values(env_path)

# Parámetros de conexión
conn_params = {
  'host': 'redshift-pda-cluster.cnuimntownzt.us-east-2.redshift.amazonaws.com',
  'database': 'pda',
  'user': credentials['user'],
  'password': credentials['password'],
  'port': 5439,
}

schema = f"{conn_params['user']}_schema"

# Conexión a la base de datos Redshift
conn = redshift_connector.connect(**conn_params)

# Mapeo de archivos y tablas
files_to_tables = {
  'station_info_procesada.csv': 'stations_info',
  'station_status_procesada.csv': 'stations_status',
  'station_availability.csv': 'stations_availability',
  'station_free_bikes.csv': 'stations_free_bikes',
}

# Función para subir a Redshift
def upload_to_redshift(file_name, table_name, mode):
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

# Subir archivos a tablas
upload_to_redshift('station_info_procesada.csv', 'stations_info', 'overwrite') # stations_info es el único que debería sobreescribirse siempre con el SCD
for file_name, table_name in files_to_tables.items():
  if table_name != 'stations_info':
    upload_to_redshift(file_name, table_name, 'append')

print("Todo subido a Redshift!! =)")