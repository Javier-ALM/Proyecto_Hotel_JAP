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
        NULLIF(TRIM(C5), '') AS direccion,
        LOWER(TRANSLATE(TRIM(REGEXP_REPLACE(REGEXP_REPLACE(C6, '_[0-9]+@', '@'), '_[0-9]+$', '')), 'áéíóúÁÉÍÓÚ',
            'aeiouAEIOU')) AS email,
        REGEXP_REPLACE(TRIM(C7), '[^0-9+]', '') AS telefono,
        COALESCE(TRY_TO_DATE(LEFT(C8, 10), 'YYYY/MM/DD'), TRY_TO_DATE(LEFT(C8, 10), 'DD/MM/YYYY')) AS fecha_regitro,
        CURRENT_TIMESTAMP() AS _dbt_loaded_at
    FROM src_clientes
)

SELECT * FROM transformacion