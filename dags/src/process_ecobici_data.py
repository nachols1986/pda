import pandas as pd
import os

def process_ecobici_data():
    # Verificar si AIRFLOW_HOME está definido, si no usar un path local
    if 'AIRFLOW_HOME' in os.environ:
        path = os.environ['AIRFLOW_HOME']
    else:
        path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))

    data_dir = f'{path}/dags/data/raw/'
    data_clean_dir = f'{path}/dags/data/clean/'

    # Cargar los datos previamente descargados
    df_info = pd.read_csv(f'{data_dir}/station_info.csv')
    df_st = pd.read_csv(f'{data_dir}/station_status.csv')

    # Mantener solo las columnas station_id, name, lat, lon, address y capacity
    df_info = df_info[['station_id', 'name', 'lat', 'lon', 'address', 'capacity']]

    # Limpiamos columnas que no interesan en df_st
    df_st.drop(['is_charging_station', 'is_installed', 'is_renting', 'is_returning', 'traffic',
                'num_bikes_available_types.mechanical', 'num_bikes_available_types.ebike'], axis=1, inplace=True)

    # Eliminamos duplicados
    df_info.drop_duplicates(inplace=True)
    df_st.drop_duplicates(inplace=True)

    # Convierto unix timestamp a datetime
    df_st['last_reported'] = pd.to_datetime(df_st['last_reported'], origin='unix', unit='s')
    df_st['last_reported'] = df_st['last_reported'] + pd.Timedelta(hours=-3)

    # Agregamos columnas para SCD
    df_info['current'] = 1
    df_st['last_refresh'] = pd.Timestamp.now()

    # Busco el archivo existente de estaciones
    df_existing = pd.read_csv(f'{data_clean_dir}/station_info_procesada.csv')

    # Comparar nuevos datos con los existentes
    for index, row in df_info.iterrows():
        station_id = row['station_id']
        
        if station_id in df_existing['station_id'].values:
            existing_row = df_existing[df_existing['station_id'] == station_id].iloc[0]
            
            row_dict = row.drop(labels='current').to_dict()
            existing_row_dict = existing_row.drop(labels='current').to_dict()
            
            if row_dict != existing_row_dict:
                df_existing.loc[df_existing['station_id'] == station_id, 'current'] = 0
                
                df_existing = pd.concat([df_existing, row.to_frame().T], ignore_index=True)
        else:
            df_existing = pd.concat([df_existing, row.to_frame().T], ignore_index=True)

    # Guardar los DataFrames en archivos CSV
    df_existing.to_csv(f'{data_clean_dir}/station_info_procesada.csv', index=False)
    df_st.to_csv(f'{data_clean_dir}/station_status_procesada.csv', index=False)

    print("Se preprocesaron los datos de los archivos station_info.csv y station_status.csv")
    print("\n---------------------------------------------------------")
    print(df_existing.head(3))
    print("---------------------------------------------------------")
    print(df_st.head(3))

# Invocar la función si se ejecuta como un script
if __name__ == '__main__':
    process_ecobici_data()
