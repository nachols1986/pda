import os
import pandas as pd

def process_station_data():
    # Verificar si AIRFLOW_HOME está definido, si no usar un path local
    if 'AIRFLOW_HOME' in os.environ:
        path = os.environ['AIRFLOW_HOME']
    else:
        path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
    
    data_dir = f'{path}/dags/data/raw/'
    data_clean_dir = f'{path}/dags/data/clean/'

    # Cargar los datos previamente procesados
    df_info = pd.read_csv(f'{data_clean_dir}/station_info_procesada.csv')
    df_st = pd.read_csv(f'{data_clean_dir}/station_status_procesada.csv')

    # Filtrar el DataFrame para quedarse solo con current = 1 de la SCD
    df_info_current = df_info[df_info['current'] == 1]

    # 1. Cantidad de estaciones fuera de servicio por fecha
    df_est_oos = df_st.groupby(['last_refresh', 'status'])['station_id'].count()
    df_est_oos = df_est_oos.unstack(fill_value=0)
    df_est_oos.reset_index(inplace=True)

    # Calcular el porcentaje de END_OF_LIFE sobre el total (EOL + IS)
    if 'END_OF_LIFE' in df_est_oos.columns and 'IN_SERVICE' in df_est_oos.columns:
        total = df_est_oos['END_OF_LIFE'].sum() + df_est_oos['IN_SERVICE'].sum()
        if total > 0:
            df_est_oos['%_END_OF_LIFE'] = round((df_est_oos['END_OF_LIFE'] / total) * 100, 2)
        else:
            df_est_oos['%_END_OF_LIFE'] = 0
    else:
        df_est_oos['%_END_OF_LIFE'] = 0

    # 2. Quiero para cada estación (guardando por fecha) su % de bicicletas disponibles
    df_merge = pd.merge(df_st, df_info_current, how='left', on='station_id')
    df_merge['perc_libres'] = df_merge['num_bikes_available'] / df_merge['capacity']
    df_merge = df_merge[['last_refresh', 'station_id', 'perc_libres']]

    # Guardar los DataFrames en archivos CSV
    df_est_oos.to_csv(f'{data_clean_dir}/station_availability.csv', index=False)
    df_merge.to_csv(f'{data_clean_dir}/station_free_bikes.csv', index=False)

    print("Se guardaron los datos de métricas de estaciones fuera de servicio y porcentaje de disponibilidad")
    print("\n---------------------------------------------------------")
    print(df_est_oos.head(3))
    print("---------------------------------------------------------")
    print(df_merge.head(3))

# Invocar la función si se ejecuta como un script
if __name__ == '__main__':
    process_station_data()
