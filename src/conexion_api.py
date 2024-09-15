import requests
import pandas as pd
import time
import os
from dotenv import dotenv_values

# Luego definiré el path desde airflow
# path = os.environ['AIRFLOW_HOME']
# También puedo probar con variables de airflow en lugar de .env...

path = "h:/My Drive/PDA/ecobici/"

# Definir la ruta relativa para el archivo .env
env_path = f'{path}/env/gcba_api_key.env'

# Definir path relativo para los datos
data_dir = f'{path}/data/raw'

def load_credentials(env_path):
    """Carga las credenciales de la API desde un .env"""
    try:
        # Leer las credenciales desde el archivo .env
        credentials = dotenv_values(env_path)
        vclient_id = credentials.get('vclient_id')
        vclient_secret = credentials.get('vclient_secret')

        if not vclient_id or not vclient_secret:
            raise ValueError("No se encontraron vclient_id o vclient_secret en el archivo .env")

        return vclient_id, vclient_secret

    except FileNotFoundError as e:
        print(f"Error: {e} - No se encontró el archivo .env en la ruta especificada.")
        raise
    except ValueError as e:
        print(f"Error: {e}")
        raise

def make_request(session, url, retries=3):
    """
    Realiza una solicitud HTTP GET utilizando una sesión de requests, con reintentos en caso de error.
    
    Args:
        session (requests.Session): Sesión HTTP persistente.
        url (str): URL para la solicitud.
        retries (int): Cantidad de intentos en caso de fallar.
        
    Returns:
        list: Lista de estaciones obtenida del JSON de respuesta.
        None: Si la solicitud falla después de varios intentos.
    """
    for attempt in range(retries):
        try:
            response = session.get(url, params=params)
            response.raise_for_status()  # Lanza excepción si hay error HTTP
            return response.json()['data']['stations']  # Extrae los datos de las estaciones
        except requests.exceptions.HTTPError as e:
            print(f"Error HTTP en intento {attempt+1}: {e}")
        except requests.exceptions.RequestException as e:
            print(f"Error de red en intento {attempt+1}: {e}")
        time.sleep(2)  # Espera antes de intentar nuevamente
    print(f"Error: No se pudo obtener la información de {url} después de {retries} intentos.")
    return None

def save_to_csv(data, filename):
    """
    Guarda los datos en un archivo CSV.
    
    Args:
        data (list): Lista de datos de las estaciones.
        filename (str): Nombre del archivo CSV a guardar.
    """
    df = pd.json_normalize(data)  # Convierte la lista a un DataFrame
    df.to_csv(filename, index=False)
    print(f"Se ha guardado la información en {filename}")

# URLs para obtener información y estado de las estaciones
urls = {
    'station_info.csv': 'https://apitransporte.buenosaires.gob.ar/ecobici/gbfs/stationInformation',
    'station_status.csv': 'https://apitransporte.buenosaires.gob.ar/ecobici/gbfs/stationStatus'
}

# Cargar credenciales
vclient_id, vclient_secret = load_credentials(env_path)

# Parámetros para las solicitudes HTTP
params = {
    'client_id': vclient_id,
    'client_secret': vclient_secret
}

# Usar una sesión para todas las solicitudes
with requests.Session() as session:
    for filename, url in urls.items():
        data = make_request(session, url)
        if data:
            save_to_csv(data, os.path.join(data_dir, filename))
        else:
            print(f"No se pudieron obtener los datos de {filename}")

# Obtener la cantidad de estaciones
if 'station_info.csv' in os.listdir(data_dir):
    df_info = pd.read_csv(os.path.join(data_dir, 'station_info.csv'))
    largo = len(df_info)
    print(f'Descargada información de {largo} estaciones')
else:
    print("No se encontró el archivo station_info.csv para contar las estaciones.")