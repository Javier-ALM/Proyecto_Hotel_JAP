{{ config(
    materialized='view'
)}}

with src_clientes AS (
    SELECT * FROM {{ source('hotel_raw', 'RAW_CLIENTE') }}
    WHERE C1 != 'id_cliente'
),

transformacion AS (
    SELECT
        CAST(C1 AS INTEGER) AS id_cliente,
        INITCAP(TRIM(C2)) AS nombre,
        UPPER(REGEXP_REPLACE(REGEXP_REPLACE(TRIM(C3), '_[0-9]+$', ''), '[^a-zA-Z0-9]', '')) as dni_pasaporte,
        UPPER(TRIM(C4)) AS nacionalidad,
        CASE 
            WHEN TRIM(C5) = '' OR TRIM(C5) IS NULL OR UPPER(TRIM(C5)) IN ('N/A', 'NULL') 
            THEN 'Dirección no disponible' 
            ELSE TRIM(C5) 
          END AS direccion,
        LOWER(TRANSLATE(
            TRIM(REGEXP_REPLACE(REGEXP_REPLACE(C6, '_[0-9]+@', '@'), '_[0-9]+$', '')),
            'áéíóúÁÉÍÓÚ',
            'aeiouAEIOU'
          )) AS email,
        '+' || REGEXP_REPLACE(TRIM(C7), '[^0-9]', '') AS telefono,
        COALESCE(
            TRY_TO_DATE(REGEXP_REPLACE(TRIM(C8), ' .*', ''), 'YYYY-MM-DD'),
            TRY_TO_DATE(REGEXP_REPLACE(TRIM(C8), ' .*', ''), 'DD/MM/YYYY'),
            TRY_TO_DATE(REGEXP_REPLACE(TRIM(C8), ' .*', ''), 'YYYY/MM/DD'),
            TO_DATE('1900-01-01')
          ) AS fecha_registro,
        
        CURRENT_TIMESTAMP() AS _dbt_loaded_at
    FROM src_clientes
)

SELECT * FROM transformacion