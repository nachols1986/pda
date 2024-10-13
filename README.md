## Descripción del trabajo

El proyecto se enfoca en la construcción de un pipeline de ETL utilizando Docker, Airflow, Python y una base de datos en Amazon Redshift. El objetivo principal es automatizar el flujo de datos desde la API Transporte del Gobierno de la Ciudad de Buenos Aires hasta una base de datos centralizada, permitiendo su posterior análisis y visualización.

El proceso comienza con la descarga de datos desde la API, la cual proporciona información detallada sobre el estado de las estaciones de las bicicletas públicas, así como datos generales sobre las mismas. Estos datos se adquieren periódicamente y se almacenan para su posterior procesamiento.

Una vez obtenidos, los datos pasan por un proceso de transformación, donde se aplican diferentes operaciones para limpiar, estructurar y enriquecer la información. Esto incluye la generación de métricas agregadas relevantes, como la disponibilidad de bicicletas en cada estación y la cantidad de estaciones fuera de servicio en un determinado período de tiempo.

Finalmente, los datos transformados se cargan en una base de datos en Amazon Redshift. En paralelo, se envía por mail a una lista de distribución a determinar, el estado del sistema y una altera si la cantidad de estaciones fuera de servicio (%) supera cierto umbral definido por el usuario.

Todo el proceso está orquestado mediante Airflow.

En resumen, este proyecto ofrece una solución completa para la extracción, transformación y carga de datos de la API de Transporte del Gobierno de la Ciudad de Buenos Aires en una base de datos en Amazon Redshift, utilizando tecnologías modernas y probadas en la industria para garantizar la eficacia y escalabilidad del pipeline de datos.

---
### Instructivo para ejecutar el proceso ETL en Airflow con Docker

#### Paso 1: Preparación de archivos

1. Descarga todos los archivos necesarios para el proceso ETL.
2. Coloca los archivos descargados en una carpeta llamada `ecobici` en tu sistema.

#### Paso 2: Organización de archivos

1. Copia el archivo Dockerfile, docker-compose.yaml y requirements.txt en la carpeta `ecobici`.
2. Copia el archivo dag.py y la carpeta `src` (que contiene los scripts `.py` necesarios) en una subcarpeta llamada `dags` dentro de `ecobici`.
3. La carpeta `data` crearla y agregar dos subcarpetas: `raw` y `clean`
4. En el raíz `ecobici` copie el contenido de la carpeta `test` para poder correr tests sobre las funciones definidas en `\ecobici\dags\src\conexion_api.py` en forma local

#### Paso 3: Configuración de archivos de entorno

Dentro de la carpeta `dags`, crea una subcarpeta llamada `env` y agrega los siguientes archivos: `gcba_api_key.env`, `redshift_key.env` y `email_key.env`.

- El archivo `gcba_api_key.env` debe tener el siguiente contenido:
    ```env
    vclient_id = 'su_client_id'
    vclient_secret = 'su_cliente_secret'
    ```
    Estos valores se obtienen de forma gratuita desde [API Transporte GCBA](https://api-transporte.buenosaires.gob.ar/registro).

- El archivo `redshift_key.env` debe tener el siguiente contenido:
    ```env
    user = 'su_usuario'
    password = 'su_contraseña'
    ```

- El archivo `email_key.env` debe tener el siguiente contenido:
    ```env
    user = 'su_usuario'
    password = 'su_contraseña'
    ```
    Estas credenciales se pueden obtener con una cuenta vinculada a la [API de Gmail de Google] (https://developers.google.com/gmail/api/guides?hl=es-419)

Dentro de la carpeta `dags`, crea una subcarpeta llamada `params` y agrega los siguientes archivos: `destinatarios.txt` y `umbral.env`.

- El archivo `destinatarios.txt` debe las direcciones de email a las cuales se quiere enviar el informe. Las mismas se deben declarar una abajo de  la otra, sin ningún tipo de separador:
    ```env
    mail_ejemplo_1@email.com
    mail_ejemplo_2@email.com
    ...
    ```

- El archivo `umbral.env` debe tener el siguiente contenido:
    ```env
    umbral = [valor entero entre 0 y 100]
    ```
    El valor de umbral que se define (será un porcentaje entre 0 y 100) será el valor para el cual, de superarse el % de estaciones OOS, se informará vía email (por default, el valor será 20).

La estructura que debería quedar es la siguiente:

```
/ ecobici
├── dags
│   ├── src
│   │   ├── conexion_api.py
│   │   ├── process_ecobici_data.py
│   │   ├── create_aggregated_tables.py
│   │   ├── send_mails.py
│   │   └── upload_to_redshift.py
│   ├── env
│   │   ├── gcba_api_key.env
│   │   ├── redshift_key.env
│   │   └── email_key.env
│   ├── params
│   │   ├── destinatarios.txt
│   │   └── umbral.env
│   ├── data
│   │   ├── raw
│   │   └── clean
│   └── dag.py
├── tests
│   └── tests_conexion_api.py
├── Dockerfile
├── docker-compose.yaml
└── requirements.txt
```

>#### Pruebas Unitarias
>Para ejecutar las pruebas unitarias de forma local, sigue estos pasos desde una consola Bash o PowerShell:
>1. **Navega a la carpeta del proyecto**:
>   Abre tu consola y cambia el directorio actual a la carpeta `ecobici`, donde se encuentra tu proyecto. 
>2. **Ejecuta las pruebas**:
>   Utiliza el módulo de pruebas de Python para ejecutar las pruebas definidas en el archivo `tests_conexion_api.py`. Ejecuta el siguiente comando: `python -m unittest tests.tests_conexion_api`.

#### Paso 4: Construcción de la imagen Docker

1. Abre PowerShell o Bash en tu sistema.
2. Navega hasta la carpeta `.\ecobici`
3. Ejecuta el siguiente comando para construir la imagen Docker: `docker build -t ecobici_elt .`.

#### Paso 5: Ejecución del contenedor Docker

1. Una vez que la imagen Docker se haya construido correctamente, ejecuta el siguiente comando para iniciar el contenedor: `docker-compose up`.
2. Esto iniciará Airflow y lo hará accesible a través del navegador web en `http://localhost:8080`.

#### Paso 6: Acceso a Airflow

1. Abre un navegador web y navega a `http://localhost:8080`.
2. Inicia sesión en Airflow con las siguientes credenciales:
   - **Usuario**: ecobici
   - **Contraseña**: ecobici

#### Paso 7: Verificación del proceso ETL

1. Una vez iniciada la sesión en Airflow, verás el DAG `ecobici_dag` en la lista de DAG disponibles.
2. Activa el DAG haciendo clic en el botón de encendido.
3. Airflow comenzará automáticamente a ejecutar el DAG según la programación definida en el mismo.

---
### Tablas de la Base de Datos

#### 1. `stations_info`

Esta tabla almacena información sobre las estaciones de bicicletas.

| station_id |         name         |   lat    |   lon    |               address               | capacity | current |
|------------|----------------------|----------|----------|-------------------------------------|----------|---------|
|          2 | 002 - Retiro 1234567 | -34.5924 | -58.3747 | AV. Dr. José María Ramos Mejía 1300 |       40 |       0 |
|          3 | 003 - ADUANA         | -34.6122 | -58.3691 | Av. Paseo Colón 380                 |       28 |       1 |
|          4 | 004 - Plaza Roma     |  -34.603 | -58.3689 | Av. Corrientes 100                  |       20 |       1 |

#### 2. `stations_status`

Esta tabla registra el estado de las estaciones de bicicletas.

| station_id | num_bikes_available | num_bikes_disabled | num_docks_available | num_docks_disabled |    last_reported    |   status   |        last_refresh        |
|------------|---------------------|--------------------|---------------------|--------------------|---------------------|------------|----------------------------|
|          2 |                  16 |                  1 |                  23 |                  0 | 2024-09-25 22:17:42 | IN_SERVICE | 2024-09-26 01:18:16.146525 |
|          3 |                   0 |                 16 |                  12 |                  0 | 2024-09-25 22:14:44 | IN_SERVICE | 2024-09-26 01:18:16.146525 |
|          4 |                   1 |                  1 |                  18 |                  0 | 2024-09-25 22:15:59 | IN_SERVICE | 2024-09-26 01:18:16.146525 |

#### 3. `stations_availability`

Esta tabla muestra la disponibilidad de las estaciones.

|        last_refresh        | IN_SERVICE | %_END_OF_LIFE |
|----------------------------|------------|---------------|
| 2024-09-26 01:17:44.994071 |        368 |             0 |

#### 4. `stations_free_bikes`

Esta tabla indica la cantidad de bicicletas libres en las estaciones.

|        last_refresh        | station_id | perc_libres |
|----------------------------|------------|-------------|
| 2024-09-26 01:17:44.994071 |          2 |         0.4 |
| 2024-09-26 01:17:44.994071 |          3 |         0.0 |
| 2024-09-26 01:17:44.994071 |          4 |        0.05 |

### Tablero de Control

Con la información online se disponibiliza un tablero de control hecho en PowerBI, actualizado automáticamente 8 veces al día:

[Dashboard Ecobici](https://nachols1986.github.io/infovis/dashboard_ecobici.html)