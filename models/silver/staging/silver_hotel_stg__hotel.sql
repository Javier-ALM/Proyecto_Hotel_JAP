{{ config(
    materialized='view'
)}}
with src_hotel AS (
    SELECT * FROM {{ source('hotel_raw', 'RAW_HOTEL') }}
),
transformacion AS (
    SELECT
        id_hotel,
        TRIM(nombre) AS nombre,
        -- LIMPIEZA DE DIRECCION: Maneja NULL real y textos 'NULL'
        COALESCE(
            CASE 
                WHEN UPPER(TRIM(direccion)) IN ('NULL', 'N/A', '', 'NONE', 'UNDEFINED') THEN NULL
                ELSE TRIM(direccion)
            END,
            'Sin direccion'
        ) AS direccion,
        TRIM(ciudad) AS ciudad,
        UPPER(TRIM(pais)) AS pais,
        '+' || REGEXP_REPLACE(telefono, '[^0-9]', '') AS telefono,
        LOWER(TRIM(REGEXP_REPLACE(REGEXP_REPLACE(email, '_[0-9]+@', '@'), '[0-9]+\\.', '.'))) AS email,
        -- FECHA: Agrega fecha por defecto si falla
        COALESCE(
            TRY_TO_DATE(LEFT(fecha_creacion, 10)),
            TRY_TO_DATE(fecha_creacion, 'YYYY/MM/DD'),
            TRY_TO_DATE(fecha_creacion, 'DD/MM/YYYY'),
            CURRENT_DATE()
        ) AS fecha_creacion,
        -- ACTIVO: Maneja NULL real primero
        COALESCE(
            CASE
                WHEN UPPER(TRIM(activo)) IN ('1', 'TRUE', 'T', 'YES', 'Y') THEN TRUE
                WHEN UPPER(TRIM(activo)) IN ('0', 'FALSE', 'F', 'NO', 'N', '', 'NULL') THEN FALSE
                ELSE NULL
            END,
            FALSE
        ) AS activo,
        -- CATEGORÍA: División entera para evitar decimales
        CASE 
            WHEN CAST(REGEXP_REPLACE(TRIM(categoria), '[^0-9]', '') AS INTEGER) >= 10 
            THEN CAST(REGEXP_REPLACE(TRIM(categoria), '[^0-9]', '') AS INTEGER) / 10
            ELSE CAST(REGEXP_REPLACE(TRIM(categoria), '[^0-9]', '') AS INTEGER)
        END::INTEGER AS categoria,
        CURRENT_TIMESTAMP() AS _dbt_loaded_at
    FROM src_hotel
)
SELECT * FROM transformacion