from datetime import datetime
from datetime import timedelta
import pandas as pd
import smtplib
import os
import math
from dotenv import dotenv_values

def obtener_credenciales(path):
    """
    Lee las credenciales de un archivo .env para obtener usuario y contraseña.
    
    Args:
    path (str): Ruta del archivo .env.
    
    Returns:
    user (str): Usuario para el envío del correo.
    password (str): Contraseña del usuario.
    """
    try:
        credentials = dotenv_values(f'{path}/dags/env/email_key.env')
        user = credentials.get('user')
        password = credentials.get('password')
        if not user or not password:
            raise ValueError("Error: No se encontraron credenciales en el archivo .env.")
        return user, password
    except FileNotFoundError:
        raise FileNotFoundError("Error: No se encontró el archivo .env en la ruta especificada.")
    except ValueError as e:
        raise ValueError(e)

def obtener_umbral(path, umbral_default=20):
    """
    Lee el umbral desde un archivo .env y valida su valor.
    
    Args:
    path (str): Ruta del archivo .env.
    umbral_default (float): Umbral predeterminado si no se encuentra el valor.
    
    Returns:
    umbral (float): Umbral válido entre 0 y 100.
    """
    try:
        credentials = dotenv_values(f'{path}/dags/params/umbral.env')
        umbral_str = credentials.get('umbral')
        if umbral_str:
            umbral = float(umbral_str)
            if not 0 <= umbral <= 100:
                raise ValueError("Error: El umbral debe ser un número entre 0 y 100.")
            return umbral
        else:
            print("Advertencia: No se encontró valor para el umbral. Se usará 20% por default.")
            return umbral_default
    except ValueError as e:
        raise ValueError(e)
    except FileNotFoundError:
        raise FileNotFoundError("Error: No se encontró el archivo .env en la ruta especificada.")

def obtener_destinatarios(path):
    """
    Lee la lista de destinatarios desde un archivo de texto.
    
    Args:
    path (str): Ruta del archivo de destinatarios.
    
    Returns:
    destinatarios (list): Lista de correos electrónicos de los destinatarios.
    """
    destinatarios = []
    try:
        with open(f'{path}/dags/params/destinatarios.txt', 'r') as file:
            for line in file:
                destinatario = line.strip()
                destinatarios.append(destinatario)
        return destinatarios
    except Exception as e:
        print(f'Error al leer el archivo de destinatarios: {e}')
        return ['no_email@yahoo.com']

def enviar_mail_diario(destinatarios, user, password, texto_mail, asunto):
    """
    Envía un correo electrónico a una lista de destinatarios.

    Args:
    destinatarios (list): Lista de direcciones de correo electrónico de los destinatarios.
    user (str): Usuario del correo electrónico remitente.
    password (str): Contraseña del correo electrónico remitente.
    texto_mail (str): Cuerpo del correo electrónico a enviar.
    asunto (str): Asunto del correo electrónico.

    Returns:
    None
    """
    try:
        x = smtplib.SMTP('smtp.gmail.com', 587)
        x.starttls()
        x.login(user, password)
        subject = asunto
        body_text = texto_mail
        for destinatario in destinatarios:
            message = f'Subject: {subject}\n\n{body_text}'
            x.sendmail(user, destinatario, message.encode('utf-8'))
        x.quit()
        print('Éxito: Correo enviado a todos los destinatarios.')
    except Exception as exception:
        print(exception)
        print('Error: No se pudo enviar el correo.')

def procesar_estaciones(path, umbral):
    """
    Procesa los datos de disponibilidad de las estaciones de Ecobici y determina si enviar una alerta.

    Args:
    path (str): Ruta de los archivos.
    umbral (float): Umbral de tolerancia de bicicletas fuera de servicio.

    Returns:
    texto_mail (str): Texto del correo electrónico a enviar.
    asunto (str): Asunto del correo electrónico.
    """
    # Leer el archivo csv de estaciones fuera de servicio
    df_est_oos = pd.read_csv(f'{path}/dags/data/clean/station_availability.csv')
    
    # Me quedo con el último dato
    fila_mayor_fecha = df_est_oos[df_est_oos['last_refresh'] == df_est_oos['last_refresh'].max()]
    
    # Obtener los valores necesarios
    fecha_str = fila_mayor_fecha['last_refresh'].iloc[0]
    fecha = (datetime.strptime(fecha_str, "%Y-%m-%d %H:%M:%S.%f") - timedelta(hours=3)).strftime("%d/%m/%Y %H:%M:%S")
    porcentaje_end_of_life = fila_mayor_fecha['%_END_OF_LIFE'].iloc[0]
    total_flota = math.ceil(fila_mayor_fecha['IN_SERVICE'].iloc[0] / (1 - porcentaje_end_of_life))
    
    if porcentaje_end_of_life < umbral:
        asunto = 'Reporte Ecobici'
        texto_mail = f"El {fecha} tenemos {porcentaje_end_of_life:.2f}% de bicicletas fuera de servicio sobre una flota total de {total_flota}"
    else:
        asunto = 'ALERTA | Servicio Ecobici Comprometido'
        texto_mail = f"El {fecha} tenemos {porcentaje_end_of_life:.2f}% de bicicletas fuera de servicio!!!! \n\n Se superó el umbral máximo de {umbral}%"
    
    return texto_mail, asunto

def ejecutar_tareas_mailing():
    """
    Función principal que encapsula la ejecución de todas las tareas.
    """
    # Cargar las credenciales desde el archivo .env
    if 'AIRFLOW_HOME' in os.environ:
        path = os.environ['AIRFLOW_HOME']
    else:
        path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))

    # Obtener credenciales y umbral
    user, password = obtener_credenciales(path)
    umbral = obtener_umbral(path)
    
    # Obtener destinatarios
    destinatarios = obtener_destinatarios(path)
    
    # Procesar datos y enviar email
    texto_mail, asunto = procesar_estaciones(path, umbral)
    enviar_mail_diario(destinatarios, user, password, texto_mail, asunto)

# Llamada a la función principal dentro de la tarea de Airflow
if __name__ == "__main__":
    ejecutar_tareas_mailing()
