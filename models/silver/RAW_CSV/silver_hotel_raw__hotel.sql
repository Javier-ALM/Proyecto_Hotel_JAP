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
        COALESCE(NULLIF(NULLIF(TRIM(direccion), ''), 'N/A'), 'Sin direccion') AS direccion,
        TRIM(ciudad) AS ciudad,
        UPPER(TRIM(PAIS)) AS pais,
        '+' || REGEXP_REPLACE(telefono, '[^0-9]', '') AS telefono,
        LOWER(TRIM(REGEXP_REPLACE(REGEXP_REPLACE(email, '_[0-9]+@', '@'), '[0-9]+\\.', '.'))) AS email,
        COALESCE(
            TRY_TO_DATE(LEFT(fecha_creacion, 10)),
            TRY_TO_DATE(fecha_creacion, 'YYYY/MM/DD'),
            TRY_TO_DATE(fecha_creacion, 'DD/MM/YYYY')
        ) AS fecha_creacion,
        CASE
            WHEN UPPER(TRIM(activo)) IN ('1', 'TRUE', 'T', 'YES', 'Y') THEN TRUE
            WHEN UPPER(TRIM(activo)) IN ('0', 'FALSE', 'F', 'NO', 'N', '') THEN FALSE
            ELSE NULL
        END AS ACTIVO,
        CAST(CAST(REGEXP_REPLACE(TRIM(categoria), '[^0-9]', '') AS INTEGER) / 10 AS INTEGER) AS categoria,
        CURRENT_TIMESTAMP() AS _dbt_loaded_at
    FROM src_hotel
)

SELECT * FROM transformacion