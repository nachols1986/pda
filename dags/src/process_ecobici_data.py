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
    df_info = pd.read_csv(f'{data_dir}/station_info.csv').assign(
        name=lambda x: x['name'].str.replace(',', '', regex=False),
        address=lambda x: x['address'].str.replace(',', '', regex=False)
    )
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

    # Cargar el archivo existente station_info_procesada.csv si existe
    file_processed = f'{data_clean_dir}/station_info_procesada.csv'
    if os.path.exists(file_processed):
        df_existing = pd.read_csv(file_processed)
    else:
        df_existing = pd.DataFrame(columns=['station_id', 'name', 'lat', 'lon', 'address', 'capacity', 'current'])

    # Separar df_existing en activos (current = 1) e inactivos (current = 0)
    df_active = df_existing[df_existing['current'] == 1].copy()
    df_inactive = df_existing[df_existing['current'] == 0].copy()

    # Redondear también en df_active para comparar
    df_active['lat'] = df_active['lat'].round(6)
    df_active['lon'] = df_active['lon'].round(6)

    # Comparar df_info con df_active
    df_merge = pd.merge(df_info, df_active, on='station_id', suffixes=('_new', '_existing'), how='left')

    # Filtrar filas donde haya cambios en alguna columna excepto 'current'
    cambios = df_merge[
        (df_merge['name_new'] != df_merge['name_existing']) |
        (df_merge['lat_new'] != df_merge['lat_existing']) |
        (df_merge['lon_new'] != df_merge['lon_existing']) |
        (df_merge['address_new'] != df_merge['address_existing']) |
        (df_merge['capacity_new'] != df_merge['capacity_existing'])
    ]

    registros_actualizados = 0
    if not cambios.empty:
        # Marcar las filas anteriores como 'current = 0'
        df_active.loc[df_active['station_id'].isin(cambios['station_id']), 'current'] = 0

        # Crear un DataFrame con las nuevas filas (current = 1)
        nuevos_registros = cambios[['station_id', 'name_new', 'lat_new', 'lon_new', 'address_new', 'capacity_new']].copy()
        nuevos_registros.columns = ['station_id', 'name', 'lat', 'lon', 'address', 'capacity']
        nuevos_registros['current'] = 1

        # Actualizar el conteo de registros actualizados
        registros_actualizados = len(nuevos_registros)

        # Combinar los nuevos registros con los inactivos y los activos
        df_existing_actualizado = pd.concat([df_active, nuevos_registros, df_inactive], ignore_index=True)
    else:
        # Si no hay cambios, combinar df_active con df_inactive sin cambios
        df_existing_actualizado = pd.concat([df_active, df_inactive], ignore_index=True)

    # Ordenar por station_id
    df_existing_actualizado.sort_values(by='station_id', inplace=True)
    
    # Guardar el DataFrame actualizado en station_info_procesada.csv
    df_existing_actualizado.to_csv(file_processed, index=False)
    
    df_st.to_csv(f'{data_clean_dir}/station_status_procesada.csv', index=False)

    # Mostrar los resultados
    print("Se preprocesaron los datos de los archivos station_info.csv y station_status.csv")
    print("\n-----------------------STATION INFO----------------------------------")
    print(f"Registros actualizados/agregados: {registros_actualizados}")
    print(df_existing.head(3))
    print("\n-------------------------STATION STATUS--------------------------------")
    print(df_st.head(3))

# Invocar la función si se ejecuta como un script
if __name__ == '__main__':
    process_ecobici_data()
