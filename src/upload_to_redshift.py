from dotenv import dotenv_values
import os
import pandas as pd
import sqlalchemy as sa
from sqlalchemy.engine.url import URL
from sqlalchemy import create_engine
import redshift_connector


# Luego definiré el path desde airflow
# path = os.environ['AIRFLOW_HOME']
# También puedo probar con variables de airflow en lugar de .env...

"""
path = "h:/My Drive/PDA/ecobici/"

# Definir la ruta relativa para el archivo .env
env_path = f'{path}/env/gcba_api_key.env'

# Definir path relativo para los datos
data_dir = f'{path}/data/raw'
"""

base_path = os.path.dirname(os.path.abspath(__file__))
data_clean_dir = os.path.join(base_path, '..', 'data', 'clean')

# Leo las credenciales
env_path = os.path.join(base_path, '..', 'env', 'redshift_key.env')
user_Credential_from_envfile = dotenv_values(env_path)
host = 'redshift-pda-cluster.cnuimntownzt.us-east-2.redshift.amazonaws.com'
database='pda'
user = user_Credential_from_envfile['user']
schema = user + '_schema'
password = user_Credential_from_envfile['password']
port = 5439

# Conexión a la base de datos RS
url = URL.create(
    drivername='redshift+redshift_connector',
    host=host,
    port=port,
    database=database,
    username=user,
    password=password
)

engine = sa.create_engine(url)

# Leer los archivos CSV creados en las partes anteriores y cargarlos en Redshift
df_info =       pd.read_csv(f'{data_clean_dir}/station_info_procesada.csv')
df_st =         pd.read_csv(f'{data_clean_dir}/station_status_procesada.csv')
df_est_oos =    pd.read_csv(f'{data_clean_dir}/station_availability.csv')
df_merge =      pd.read_csv(f'{data_clean_dir}/station_free_bikes.csv')

# Insertar todo el contenido de los DataFrames en las tablas correspondientes en Redshift
table_name = 'stations_info'
df_info.to_sql(table_name, engine, index=False, if_exists='append')

table_name = 'stations_status'
df_st.to_sql(table_name, engine, index=False, if_exists='append')

table_name = 'stations_availability'
df_est_oos.to_sql(table_name, engine, index=False, if_exists='append')

table_name = 'stations_free_bikes'
df_merge.to_sql(table_name, engine, index=False, if_exists='append')

print("Todo subido a Redshift!! =)")