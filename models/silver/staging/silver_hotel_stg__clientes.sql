{{ config(
    materialized='view'
)}}

with src_snapshot AS (
    -- Traemos todo lo que hay en la snapshot
    -- dbt se encarga de buscar la tabla correcta (aunque sea en tu esquema de dev)
    SELECT * FROM {{ ref('snp_clientes') }}
)

SELECT
    -- Columnas de negocio
    "ID_CLIENTE" as id_cliente,
    "NOMBRE" as nombre,
    "DNI_PASAPORTE" as dni_pasaporte,
    "NACIONALIDAD" as nacionalidad,
    "DIRECCION" as direccion,
    "EMAIL" as email,
    "TELEFONO" as telefono,
    "FECHA_REGISTRO" as fecha_registro,
    
    -- Metadatos de dbt con comillas dobles (como funcionó en Snowflake)
    "DBT_UPDATED_AT" as dbt_updated_at,
    "DBT_VALID_FROM" as dbt_valid_from,
    
    -- Nuestra columna de carga
    CURRENT_TIMESTAMP() AS _dbt_loaded_at
FROM src_snapshot
-- Filtramos usando las comillas que dieron éxito en tu prueba
WHERE "DBT_VALID_TO" IS NULL