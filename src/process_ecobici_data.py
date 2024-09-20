import pandas as pd
import os

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

# Definir la ruta relativa para los datos
data_dir = os.path.join(base_path, '..', 'data', 'raw')
data_clean_dir = os.path.join(base_path, '..', 'data', 'clean')

# Cargar los datos previamente descargados
df_info = pd.read_csv(f'{data_dir}/station_info.csv')
df_st = pd.read_csv(f'{data_dir}/station_status.csv')

# Mantener solo las columnas station_id, name, lat, lon, address y capacity
df_info = df_info[['station_id', 'name', 'lat', 'lon', 'address', 'capacity']]

# Limpiamos columnas que no interesan en df_st
df_st.drop(['is_charging_station', 'is_installed', 'is_renting', 'is_returning', 'traffic',
            'num_bikes_available_types.mechanical', 'num_bikes_available_types.ebike'], axis=1, inplace=True)

# 1. Eliminamos duplicados
df_info.drop_duplicates(inplace=True)
df_st.drop_duplicates(inplace=True)

# Convierto unix timestamp a datetime
df_st['last_reported'] = pd.to_datetime(df_st['last_reported'], origin='unix', unit='s')
df_st['last_reported'] = df_st['last_reported'] + pd.Timedelta(hours=-3)

# Agrego fecha de reporte de la info --> para el df_info siempre se va a sobreescribir la tabla || para df_st se irá acumulando el status

# Agregamos columnas para SCD
df_info['current'] = 1

# En el de status, simplemente, el momento de actualizacion
df_st['last_refresh'] = pd.Timestamp.now()

# Busco el archivo existente de estaciones
df_existing = pd.read_csv(f'{data_clean_dir}/station_info_procesada.csv')

# Comparar nuevos datos con los existentes
for index, row in df_info.iterrows():
    station_id = row['station_id']
    
    # Si la estación ya existe en la tabla dimensional
    if station_id in df_existing['station_id'].values:
        existing_row = df_existing[df_existing['station_id'] == station_id].iloc[0]
        
        # Comparar las filas excluyendo 'current'
        row_dict = row.drop(labels='current').to_dict()
        existing_row_dict = existing_row.drop(labels='current').to_dict()
        
        # Si hay diferencias en los valores
        if row_dict != existing_row_dict:
            # Actualizar el registro existente como no actual (current=0)
            df_existing.loc[df_existing['station_id'] == station_id, 'current'] = 0
            
            # Agregar el nuevo registro
            df_existing = pd.concat([df_existing, row.to_frame().T], ignore_index=True)
    else:
        # Si la estación no existe, agregar el nuevo registro
        df_existing = pd.concat([df_existing, row.to_frame().T], ignore_index=True)

# Guardar los DataFrames en archivos CSV
df_existing.to_csv(f'{data_clean_dir}/station_info_procesada.csv', index=False)

# Guardar station_status_procesada appendeando
station_status_file = f'{data_clean_dir}/station_status_procesada.csv'

# Verificar si el archivo ya existe para decidir si escribir el encabezado o no
if not os.path.isfile(station_status_file):
    df_st.to_csv(station_status_file, index=False, mode='w', header=True)  # Crear el archivo con el encabezado si no existe
else:
    df_st.to_csv(station_status_file, index=False, mode='a', header=False)  # Appendeando sin el encabezado si ya existe

print("Se preprocesaron los datos de los archivos station_info.csv y station_status.csv")
print("\n---------------------------------------------------------")
print(df_existing.head(3))
print("---------------------------------------------------------")
print(df_st.head(3))