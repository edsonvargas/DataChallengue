
# Data Challengue: Dublin Transportation Data Warehouse

En este código se establece un pipeline de ingesta de datos en Snowflake utilizando integraciones con Amazon S3 y Azure Blob Storage. A continuación, se detallan los pasos y las razones para la elección de cada herramienta.

## 1. Ingesta de número de pasajeros en las líneas de transporte de Irlanda

Se debe ingestar la siguiente información https://data.gov.ie/dataset/toa11-luas-passenger-numbers, Para ello, re realizó la carga del csv, al S3 de amazon, se configuró en IAM, la política y el rol con el acceso al bucket. 

### 1. Configuración Inicial

```sql
USE ROLE accountadmin;
USE WAREHOUSE compute_wh;

-- Crear una base de datos y un esquema para los datos de transporte.
CREATE OR REPLACE DATABASE db_ravn_test;
CREATE OR REPLACE SCHEMA db_ravn_test.staging_pos;
```

Se utiliza el rol `accountadmin` y el `warehouse` para asegurar la correcta administración y el almacenamiento eficiente de los recursos. La base de datos y esquema son creados para organizar los datos de transporte.

```sql
-- Crear la tabla raw para almacenar los datos de transporte.
CREATE OR REPLACE TABLE db_ravn_test.staging_pos.transportation
(
    statistic VARCHAR(16777216),
    statisticLabel VARCHAR(16777216),
    tlist NUMBER(4,0),
    anio NUMBER(4,0),
    mes NUMBER(2,0),
    mesDesc VARCHAR(16777216),
    unit VARCHAR(16777216),
    value NUMBER
);
```
Se define una tabla que contendrá los datos relacionados con estadísticas de transporte, incluyendo el año, mes, unidades y valores de las métricas.
### Integración con AWS S3
```sql
-- Crear la integración de almacenamiento para acceder a los datos en Amazon S3.
CREATE STORAGE INTEGRATION transportation_si
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = 'S3'
    ENABLED = true
    STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::522814735743:role/dublin-si-role'
    STORAGE_ALLOWED_LOCATIONS = ('s3://dublintransportationdwh');
```
Se configura una integración con Amazon S3 para extraer los datos de transporte desde un bucket específico.

### Creación de formatos de archivo
```sql
-- Definir un formato de archivo para leer archivos CSV desde S3.
CREATE OR REPLACE FILE FORMAT mycsvformat
   TYPE = 'CSV'
   FIELD_DELIMITER = ','
   FIELD_OPTIONALLY_ENCLOSED_BY = '"'
   SKIP_HEADER = 1;

-- Definir un segundo formato de archivo para leer archivos con delimitador pipe '|'.
CREATE OR REPLACE FILE FORMAT mycsvformat_station
   TYPE = 'CSV'
   FIELD_DELIMITER = '|'
   FIELD_OPTIONALLY_ENCLOSED_BY = '"'
   SKIP_HEADER = 1;
```
Se definen formatos de archivo personalizados para CSV, permitiendo la ingesta de datos con diferentes delimitadores, dependiendo de la estructura del archivo fuente. Se uso | como delimitador, debido a que el archivo contenía campos con comas, lo cual no permitía una ingesta correcta, se pudo haber remediado aplicando expresión regular o condicionales, pero esto no controlaría todos los casos que pudieran suceder, es recomendable realizar un preprocesamiento del archivo en python u otro lenguaje.

### Creación de Etapa de Almacenamiento en S3
```sql
-- Crear la etapa para acceder a los archivos de transporte en S3. 
CREATE OR REPLACE STAGE transportation_stage_integration url = 's3://dublintransportationdwh/' 
	STORAGE_INTEGRATION = transportation_si file_format = mycsvformat;
```
La etapa (stage) se utiliza para especificar el bucket de S3 y el formato de los archivos que se cargarán en Snowflake.

### Carga de Datos desde S3 a la Tabla de Transporte
```sql
-- Listar los archivos en la etapa.
LIST @transportation_stage_integration;

-- Cargar datos en la tabla de transporte.
COPY INTO db_ravn_test.staging_pos.transportation
    FROM
    (
      SELECT
        $1,$2,TRY_TO_NUMBER($3),TRY_TO_NUMBER($4),TRY_TO_NUMBER($5),$6,$7,TRY_TO_NUMBER($8)
        FROM @transportation_stage_integration
    )
    ON_ERROR = 'skip_file';
```
Se realiza la ingesta de los datos desde S3 a la tabla de transporte, aplicando conversiones de tipo en los datos numéricos y saltando archivos corruptos.

## 2. Ingesta de estaciones climáticas Irlanda.

Se debe ingestar data climática de las estaciones en Irlanda, la data se encuentra en el siguiente enlace https://www.met.ie/climate/available-data/historical-data, Para ello, re realizó la carga del csv, al blob storage de amazon, se configuró el container y la cuenta de storage y se brindó el acceso desde mi cuenta personal de microsoft.
```sql
-- Crear integración para leer datos de Azure Blob Storage.
CREATE STORAGE INTEGRATION station_si
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = AZURE
    ENABLED = TRUE
    AZURE_TENANT_ID = '5c02c5f8-f09c-41ca-b65b-d34981f299cc'
    STORAGE_ALLOWED_LOCATIONS = ('azure://snowflake0001.blob.core.windows.net/stationcontainer');
```
### Integración con Azure Blob Storage

```sql
-- Crear integración para leer datos de Azure Blob Storage.
CREATE STORAGE INTEGRATION station_si
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = AZURE
    ENABLED = TRUE
    AZURE_TENANT_ID = '5c02c5f8-f09c-41ca-b65b-d34981f299cc'
    STORAGE_ALLOWED_LOCATIONS = ('azure://snowflake0001.blob.core.windows.net/stationcontainer');
```
Se configura una integración con Azure Blob Storage.

### Creación de la Tabla de Estaciones
```sql
-- Crear la tabla para almacenar los datos de estaciones de bicicletas.
CREATE OR REPLACE TABLE db_ravn_test.staging_pos.station
(
    county VARCHAR(16777216),
    station_name NUMBER,
    name VARCHAR(16777216),
    height NUMBER(10,0),
    easting NUMBER(10,0),
    northing NUMBER(10,0),
    latitude DOUBLE,
    longitude DOUBLE,
    open_year NUMBER(4,0),
    close_year NUMBER(4,0)
);
```
Se ingestan los datos de las estaciones desde Azure, aplicando las conversiones necesarias.

```sql
-- Cargar datos desde Azure Blob Storage a la tabla de estaciones.
COPY INTO db_ravn_test.staging_pos.station
    FROM
    (
      SELECT
        $1,TRY_TO_NUMBER($2),$3,TRY_TO_NUMBER($4),TRY_TO_NUMBER($5),
        TRY_TO_NUMBER($6),TRY_TO_DECIMAL($7),TRY_TO_DECIMAL($8),
        TRY_TO_NUMBER($9),TRY_TO_NUMBER($10)
        FROM @station_stage_integration
    )
    ON_ERROR = 'skip_file';

```
## 3. Ingesta de histórico de bicicletas por estación en Dublín.

Se debe ingestar data histórica de estado de bicicletas en las estaciones de dublín, Irlanda, la data se encuentra en el siguiente enlace https://data.gov.ie/dataset/dublinbikes-api/resource/6b2e97bd-3221-40a0-bb76-54c473a91b11, Para ello, se realizó dos ejercicios, haciendo la carga desde los CSVs disponibles en el sitio y desde formato JSON, a partir del endpoint https://data.smartdublin.ie/dublinbikes-api/bikes/dublin_bikes/historical/stations?dt_start=2024-07-01&dt_end=2024-10-01.

### Ejercicio de carga desde CSV con múltiples archivos
La carga se realizó a otro container de AZURE, se utilizó un storage integration distinto para apuntar al nuevo container, esto por temas de mantenimiento. 
```sql
CREATE STORAGE INTEGRATION station_historical_si
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = AZURE
    ENABLED = TRUE
    AZURE_TENANT_ID = '5c02c5f8-f09c-41ca-b65b-d34981f299cc'
    STORAGE_ALLOWED_LOCATIONS = ('azure://snowflake0001.blob.core.windows.net/historicalstation')
    ;

DESC STORAGE INTEGRATION station_historical_si


```
Luego , se creo la tabla para almacenar la información cargada, para este caso se utilizó el primer formato creado en este ejercicio.

```sql
-- Crear la tabla para almacenar datos históricos de estaciones.

CREATE OR REPLACE STAGE historical_station_stage_integration
    url = 'azure://snowflake0001.blob.core.windows.net/historicalstation'
    STORAGE_INTEGRATION = station_historical_si
    file_format = mycsvformat;

LIST @historical_station_stage_integration;

CREATE OR REPLACE TABLE db_ravn_test.staging_pos.historicalbike_station
(
    system_id VARCHAR(16777216),                 
    last_reported TIMESTAMP,                     
    station_id VARCHAR(16777216),                
    num_bikes_available NUMBER(5,0),             
    num_docks_available NUMBER(5,0),             
    is_installed BOOLEAN,                        
    is_renting BOOLEAN,                          
    is_returning BOOLEAN,                        
    name VARCHAR(16777216),                      
    short_name VARCHAR(16777216),                
    address VARCHAR(16777216),                   
    lat DOUBLE,                                  
    lon DOUBLE,                                  
    region_id STRING,                            
    capacity INT  
);
-- Cargar datos históricos de estaciones desde Azure.
COPY INTO db_ravn_test.staging_pos.historicalbike_station
    FROM
    (
      SELECT
        $1,TO_TIMESTAMP_NTZ($2),$3,TRY_TO_NUMBER($4),TRY_TO_NUMBER($5),
        TO_BOOLEAN($6),TO_BOOLEAN($7),TO_BOOLEAN($8),$9,$10,$11,
        TRY_TO_DECIMAL($12),TRY_TO_DECIMAL($13),$14,TRY_TO_NUMBER($15)
        FROM @historical_station_stage_integration
    )
    ON_ERROR = 'skip_file';

```
Esta tabla almacena datos históricos de estaciones de bicicletas para análisis a largo plazo.

### Ejercicio de carga desde JSON
Para este ejercicio se usó el endpoint descrito en esta sección y se descargó el response.json utilizando postman, adicional a ello, se creo un nuevo container para esta nueva ingesta:
```sql
--Creación del storage integration para el nuevo container
CREATE STORAGE INTEGRATION station_historical_json_si
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = AZURE
    ENABLED = TRUE
    AZURE_TENANT_ID = '5c02c5f8-f09c-41ca-b65b-d34981f299cc'
    STORAGE_ALLOWED_LOCATIONS = ('azure://snowflake0001.blob.core.windows.net/historicalstationjson')
    ;
```
Se define un nuevo formato, debido que es json, también se especifica que es un arreglo de cadenas JSON.

```sql
CREATE OR REPLACE FILE FORMAT myjsonformat
      TYPE = 'JSON'
      STRIP_OUTER_ARRAY = TRUE 
      FILE_EXTENSION = 'json'
      COMPRESSION = 'AUTO';
   ;
```
Finalmente, se realiza la creación del stage, la tabla donde se almacenará los datos y finalmente se hace la copia. A diferencia de la carga en CSV, se debe hacer la transformación de los datos VARIANT a las respectivas columnas de la tabla creada

```sql
-- Creación del stage para la carga de datos desde Azure blob storage.
CREATE OR REPLACE STAGE historical_station_json_stage_integration
    url = 'azure://snowflake0001.blob.core.windows.net/historicalstationjson'
    STORAGE_INTEGRATION = station_historical_json_si
    file_format = myjsonformat;

LIST @historical_station_json_stage_integration;
-- Creación de la tabla histórica de bicicletas por estación.
CREATE OR REPLACE TABLE db_ravn_test.staging_pos.historical_bike_station_json
(
    system_id VARCHAR(16777216),                 
    last_reported TIMESTAMP,                     
    station_id VARCHAR(16777216),                
    num_bikes_available NUMBER(5,0),             
    num_docks_available NUMBER(5,0),             
    is_installed BOOLEAN,                        
    is_renting BOOLEAN,                          
    is_returning BOOLEAN,                        
    name VARCHAR(16777216),                      
    short_name VARCHAR(16777216),                
    address VARCHAR(16777216),                   
    lat DOUBLE,                                  
    lon DOUBLE,                                  
    region_id STRING,                            
    capacity INT  
);
-- Copia del archivo stage a la tabla en snowflake.
COPY INTO db_ravn_test.staging_pos.historical_bike_station_json
FROM (
  SELECT
    $1:system_id::STRING AS system_id,
    TO_TIMESTAMP_NTZ($1:last_reported::STRING) AS last_reported,
    $1:station_id::STRING AS station_id,
    $1:num_bikes_available::NUMBER AS num_bikes_available,
    $1:num_docks_available::NUMBER AS num_docks_available,
    $1:is_installed::BOOLEAN AS is_installed,
    $1:is_renting::BOOLEAN AS is_renting,
    $1:is_returning::BOOLEAN AS is_returning,
    $1:name::STRING AS name,
    $1:short_name::STRING AS short_name,
    $1:address::STRING AS address,
    $1:lat::DOUBLE AS lat,
    $1:lon::DOUBLE AS lon,
    $1:region_id::STRING AS region_id,
    $1:capacity::NUMBER AS capacity
  FROM @historical_station_json_stage_integration
)
ON_ERROR = 'skip_file';
```

## Hallazgos y comentarios
-	Snowflake facilita la ingesta de datos, provee la conexión con diversos tipos de almacenamiento, como en este caso, AWS, Azure.
-	Usar Storage Integration permite ser más específico en la seguridad de envío de información, además puede escalarse y se puede manejar de manera independiente cada fuente de datos.
-	En un principio intenté crear una capa staging, con los datos como string, para luego pasarlo a raw con la data transformada, pero snowflake ya tiene su propia capa staging, lo que hizo que desistiera de esta idea, la carga en staging además puede generar un log y tiene varios comandos que facilitan la ingesta.
-	Snowflake permite configurar fácilmente transformaciones de data, conversiones, valores adicionales, aunque ha sido dificil poder eliminar una coma de unos campo, he encontrado algunas maneras de lidiar con ello, insertar toda la data como registros de un solo campo y luego aplicar expresiones regulares para dividir en columnas, el problema es que esto hace que el código sea muy customizado, se pierde generalización, además del costo de procesamiento, lo ideal sería preprocesar el archivo antes de la carga, ya sea fijando un delimitador (lo que hice) o considerar las cadenas con comillas.
-	Como siguientes pasos de este ejercicio, usaría pipes para realizar ingestas automáticas desde snowflake, creación de los validadores de carga para realizar monitoreo, generación de esquemas de la tabla de manera automática, usar una nueva capa de información para combinar tablas.
