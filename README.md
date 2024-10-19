# Readme

Este repositorio contiene la implementación inicial de un sistema de **Data Warehouse** en Snowflake para cargar y procesar datos de algunas fuentes públicas de Irlanda. Los datos son cargados desde **Amazon S3** y **Azure Blob Storage** en formato CSV y JSON, y se almacenan en diferentes tablas de staging para su posterior análisis. El archivo ravndatachallengue.md contiene la documentación del código.
El archivo datachallengue.sql, es el archivo de trabajo en snowflake, para ejecutarlo, se debe asegurar la conexión con las nubes de azure y aws, actualmente solo yo tengo las credenciales, pero en caso sea necesario, puedo habilitar los permisos para que la ejecución sea fluída. En caso quiera generar la conexion con ambas plataformas aquí dejo los enlaces de la documentación:
Storage Integration
https://docs.snowflake.com/en/sql-reference/sql/create-storage-integration
Configuración de Storage Integration con S3
https://docs.snowflake.com/en/user-guide/data-load-s3-config-storage-integration
Configuración con Azure Storage
https://docs.snowflake.com/en/user-guide/data-load-azure-config
El archivo Pipeline contiene el pipeline sugerido para la ingesta automática del problema propuesto.
