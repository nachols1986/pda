import requests
import pandas as pd
import time
import os
from dotenv import dotenv_values

# Verificar si AIRFLOW_HOME está definido, si no usar un path local
if 'AIRFLOW_HOME' in os.environ:
    path = os.environ['AIRFLOW_HOME']
else:
    path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))

env_path = f'{path}/dags/env/gcba_api_key.env'
data_dir = f'{path}/dags/data/raw/'

def load_credentials(env_path=None):
    """Carga las credenciales de la API desde un archivo .env o variables de entorno."""
    
    if env_path and os.path.exists(env_path):
        credentials = dotenv_values(env_path)
    else:
        credentials = {
            'vclient_id': os.getenv('vclient_id'),
            'vclient_secret': os.getenv('vclient_secret')
        }
    
    vclient_id = credentials.get('vclient_id')
    vclient_secret = credentials.get('vclient_secret')

    if not vclient_id or not vclient_secret:
        raise ValueError("No se encontraron vclient_id o vclient_secret en el archivo .env o en las variables de entorno")
    
    return vclient_id, vclient_secret

def make_request(session, url, params, retries=3):
    """
    Realiza una solicitud HTTP GET utilizando una sesión de requests, con reintentos en caso de error.
    """
    for attempt in range(retries):
        try:
            response = session.get(url, params=params)
            response.raise_for_status()  
            return response.json()['data']['stations']
        except requests.exceptions.HTTPError as e:
            print(f"Error HTTP en intento {attempt+1}: {e}")
        except requests.exceptions.RequestException as e:
            print(f"Error de red en intento {attempt+1}: {e}")
        time.sleep(2)
    print(f"Error: No se pudo obtener la información de {url} después de {retries} intentos.")
    return None

def save_to_csv(data, filename):
    """
    Guarda los datos en un archivo CSV y muestra el número de filas guardadas.
    """
    df = pd.json_normalize(data)

    if 'lat' in df.columns and 'lon' in df.columns:
        df['lat'] = df['lat'].round(4)
        df['lon'] = df['lon'].round(4)

    print(f"Largo del archivo {filename}: {len(df)} filas")
    
    df.to_csv(filename, index=False)
    print(f"Se ha guardado la información en {filename}")

def download_station_data():
    """
    Función principal que descarga los datos de estaciones y los guarda como CSV.
    """
    urls = {
        'station_info.csv': 'https://apitransporte.buenosaires.gob.ar/ecobici/gbfs/stationInformation',
        'station_status.csv': 'https://apitransporte.buenosaires.gob.ar/ecobici/gbfs/stationStatus'
    }

    vclient_id, vclient_secret = load_credentials(env_path)

    params = {
        'client_id': vclient_id,
        'client_secret': vclient_secret
    }

    with requests.Session() as session:
        for filename, url in urls.items():
            data = make_request(session, url, params)
            if data:
                save_to_csv(data, os.path.join(data_dir, filename))
            else:
                print(f"No se pudieron obtener los datos de {filename}")

if __name__ == "__main__":
    download_station_data()
