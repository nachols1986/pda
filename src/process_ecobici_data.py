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

# Limpiamos columnas que no interesan
df_info = df_info.iloc[:, 0:9]
df_info.drop(['physical_configuration', 'altitude', 'post_code'], axis=1, inplace=True)

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
df_info['start_date'] = pd.Timestamp.now()
df_info['end_date'] = pd.NaT
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
        existing_row = df_existing[df_existing['station_id'] == station_id]
        
        # Si hay diferencias en los valores
        if not row.equals(existing_row.iloc[0, :-3]):  # Excluyendo las columnas de fechas y current
            
            # Actualizar el registro existente como no actual (current=0) y agregar end_date
            df_existing.loc[df_existing['station_id'] == station_id, 'current'] = 0
            df_existing.loc[df_existing['station_id'] == station_id, 'end_date'] = pd.Timestamp.now()
            
            # Agregar el nuevo registro
            df_existing = pd.concat([df_existing, row.to_frame().T], ignore_index=True)
    else:
        # Si la estación no existe, agregar el nuevo registro
        df_existing = pd.concat([df_existing, row.to_frame().T], ignore_index=True)

# Guardar los DataFrames en archivos CSV o en una base de datos temporal
df_existing.to_csv(f'{data_clean_dir}/station_info_procesada.csv', index=False)
df_st.to_csv(f'{data_clean_dir}/station_status_procesada.csv', index=False)

print("Se preprocesaron los datos de los archivos station_info.csv y station_status.csv")
#print(df_info.head(3))
#print(df_st.head(3))