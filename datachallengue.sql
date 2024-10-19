-- Cambia el rol actual a `accountadmin` para tener permisos administrativos.
USE ROLE accountadmin;

-- Selecciona el `warehouse` a utilizar para ejecutar las operaciones.
USE WAREHOUSE compute_wh;

-- Crea la base de datos `db_ravn_test` o reemplaza si ya existe.
CREATE OR REPLACE DATABASE db_ravn_test;

-- Crea el esquema `staging_pos` dentro de la base de datos `db_ravn_test`.
CREATE OR REPLACE SCHEMA db_ravn_test.staging_pos;

-- Crea la tabla `transportation` en el esquema `staging_pos` con las columnas correspondientes.
CREATE OR REPLACE TABLE db_ravn_test.staging_pos.transportation
(
    statistic VARCHAR(16777216),  -- Descripción del tipo de estadística.
    statisticLabel VARCHAR(16777216),  -- Etiqueta de la estadística.
    tlist NUMBER(4,0),  -- Lista de transporte.
    anio NUMBER(4,0),  -- Año de la estadística.
    mes NUMBER(2,0),  -- Mes de la estadística.
    mesDesc VARCHAR(16777216),  -- Descripción del mes.
    unit VARCHAR(16777216),  -- Unidad de medida.
    value NUMBER  -- Valor de la estadística.
);

-- Crea una integración de almacenamiento con S3 para cargar los datos desde un bucket específico.
CREATE STORAGE INTEGRATION transportation_si
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = 'S3'
    ENABLED = true
    STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::522814735743:role/dublin-si-role'
    STORAGE_ALLOWED_LOCATIONS = ('s3://dublintransportationdwh');

-- Describe la integración de almacenamiento recién creada para verificar su configuración.
DESC INTEGRATION transportation_si;

-- Crea un formato de archivo CSV con delimitador de coma y encabezado opcionalmente entre comillas.
CREATE OR REPLACE FILE FORMAT mycsvformat
   TYPE = 'CSV'
   FIELD_DELIMITER = ','
   FIELD_OPTIONALLY_ENCLOSED_BY = '"'
   SKIP_HEADER = 1;

-- Crea un formato de archivo CSV para estaciones, utilizando `|` como delimitador.
CREATE OR REPLACE FILE FORMAT mycsvformat_station
   TYPE = 'CSV'
   FIELD_DELIMITER = '|'
   FIELD_OPTIONALLY_ENCLOSED_BY = '"'
   SKIP_HEADER = 1;

-- Crea un `stage` en Snowflake vinculado al bucket S3 para cargar archivos CSV.
CREATE OR REPLACE STAGE transportation_stage_integration
    url = 's3://dublintransportationdwh/'
    STORAGE_INTEGRATION = transportation_si
    file_format = mycsvformat;

-- Lista los archivos disponibles en el stage de transporte.
LIST @transportation_stage_integration;

-- Carga los datos del stage en la tabla `transportation`, intentando convertir a tipo numérico donde sea necesario.
COPY INTO db_ravn_test.staging_pos.transportation
    FROM
    (
      SELECT
        $1,$2,TRY_TO_NUMBER($3),TRY_TO_NUMBER($4),TRY_TO_NUMBER($5),$6,$7,TRY_TO_NUMBER($8)
        FROM @transportation_stage_integration
    )
    ON_ERROR = 'skip_file';

-------------------------------------------------------------------------------------------
-- CARGA DESDE AZURE
-------------------------------------------------------------------------------------------

-- Crea una integración de almacenamiento con Azure Blob Storage.
CREATE STORAGE INTEGRATION station_si
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = AZURE
    ENABLED = TRUE
    AZURE_TENANT_ID = '5c02c5f8-f09c-41ca-b65b-d34981f299cc'
    STORAGE_ALLOWED_LOCATIONS = ('azure://snowflake0001.blob.core.windows.net/stationcontainer');

-- Describe la integración de almacenamiento de estaciones.
DESC STORAGE INTEGRATION station_si;

-- Crea un stage para cargar archivos CSV desde Azure Blob Storage usando el formato de archivo `mycsvformat_station`.
CREATE OR REPLACE STAGE station_stage_integration
    url = 'azure://snowflake0001.blob.core.windows.net/stationcontainer'
    STORAGE_INTEGRATION = station_si
    file_format = mycsvformat_station;

-- Lista los archivos en el stage de estaciones.
LIST @station_stage_integration;

-- Crea la tabla `station` para almacenar datos de estaciones meteorológicas.
CREATE OR REPLACE TABLE db_ravn_test.staging_pos.station
(
    county VARCHAR(16777216),  -- Condado de la estación.
    station_name NUMBER,  -- ID numérico de la estación.
    name VARCHAR(16777216),  -- Nombre de la estación.
    height NUMBER(10,0),  -- Altura de la estación.
    easting NUMBER(10,0),  -- Coordenada este.
    northing NUMBER(10,0),  -- Coordenada norte.
    latitude DOUBLE,  -- Latitud.
    longitude DOUBLE,  -- Longitud.
    open_year NUMBER(4,0),  -- Año de apertura.
    close_year NUMBER(4,0)  -- Año de cierre (si aplica).
);

-- Carga los datos desde el stage de estaciones en la tabla `station`.
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

---------------------------------------------------------------------------

-- Crea una integración de almacenamiento para cargar datos históricos de estaciones.
CREATE STORAGE INTEGRATION station_historical_si
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = AZURE
    ENABLED = TRUE
    AZURE_TENANT_ID = '5c02c5f8-f09c-41ca-b65b-d34981f299cc'
    STORAGE_ALLOWED_LOCATIONS = ('azure://snowflake0001.blob.core.windows.net/historicalstation');

-- Describe la integración de almacenamiento de estaciones históricas.
DESC STORAGE INTEGRATION station_historical_si;

-- Crea un stage para cargar archivos desde Azure Blob Storage para datos históricos.
CREATE OR REPLACE STAGE historical_station_stage_integration
    url = 'azure://snowflake0001.blob.core.windows.net/historicalstation'
    STORAGE_INTEGRATION = station_historical_si
    file_format = mycsvformat;

-- Lista los archivos disponibles en el stage de estaciones históricas.
LIST @historical_station_stage_integration;

-- Crea la tabla `historicalbike_station` para almacenar datos históricos de estaciones de bicicletas.
CREATE OR REPLACE TABLE db_ravn_test.staging_pos.historicalbike_station
(
    system_id VARCHAR(16777216),  -- ID del sistema de bicicletas.
    last_reported TIMESTAMP,  -- Última fecha de reporte.
    station_id VARCHAR(16777216),  -- ID de la estación.
    num_bikes_available NUMBER(5,0),  -- Número de bicicletas disponibles.
    num_docks_available NUMBER(5,0),  -- Número de docks disponibles.
    is_installed BOOLEAN,  -- Indicador de si la estación está instalada.
    is_renting BOOLEAN,  -- Indicador de si la estación está alquilando bicicletas.
    is_returning BOOLEAN,  -- Indicador de si la estación está recibiendo devoluciones.
    name VARCHAR(16777216),  -- Nombre de la estación.
    short_name VARCHAR(16777216),  -- Nombre corto.
    address VARCHAR(16777216),  -- Dirección de la estación.
    lat DOUBLE,  -- Latitud.
    lon DOUBLE,  -- Longitud.
    region_id STRING,  -- ID de la región.
    capacity INT  -- Capacidad de la estación.
);

-- Carga los datos históricos de estaciones de bicicletas en la tabla `historicalbike_station`.
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

---------------------------------------------------------------------------

-- Crea una integración de almacenamiento para cargar datos históricos de estaciones en formato JSON.
CREATE STORAGE INTEGRATION station_historical_json_si
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = AZURE
    ENABLED = TRUE
    AZURE_TENANT_ID = '5c02c5f8-f09c-41ca-b65b-d34981f299cc'
    STORAGE_ALLOWED_LOCATIONS = ('azure://snowflake0001.blob.core.windows.net/historicalstationjson');

-- Describe la integración de almacenamiento de estaciones históricas en formato JSON.
DESC STORAGE INTEGRATION station_historical_json_si;

-- Crea un formato de archivo para manejar datos en formato JSON.
CREATE OR REPLACE FILE FORMAT myjsonformat
      TYPE = 'JSON'
      STRIP_OUTER_ARRAY = TRUE 
      FILE_EXTENSION = 'json'
      COMPRESSION = 'AUTO';

-- Crea un stage para cargar datos históricos de estaciones en formato JSON desde Azure Blob Storage.
CREATE OR REPLACE STAGE historical_station_json_stage_integration
    url = 'azure://snowflake0001.blob.core.windows.net/historicalstationjson'
    STORAGE_INTEGRATION = station_historical_json_si
    file_format = myjsonformat;

LIST @historical_station_json_stage_integration;

-- Crea la tabla de almacenamiento de los datos históricos de las estaciones
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

-- Copia los datos de json y realiza un formateo apropiado para cada tipo de dato
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


-- Querys de validacion
select count(*) from  db_ravn_test.staging_pos.historical_bike_station_json
select count(*) from db_ravn_test.staging_pos.historicalbike_station