{{ config(
    materialized='view'
)}}

WITH src_consumo AS (
    SELECT * FROM {{ source('hotel_raw', 'RAW_CONSUMO') }}
),

quitar_duplicados AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY id_consumo ORDER BY id_consumo) AS row_num
    FROM src_consumo
),

transformacion AS (
    SELECT
        id_consumo::INTEGER AS id_consumo,
        id_reserva::INTEGER AS id_reserva,
        TRIM(id_servicio::TEXT) AS id_servicio,

        COALESCE (
            TRY_TO_DATE(REGEXP_REPLACE(LEFT(TRIM(fecha_consumo::TEXT), 10), '[-]', '/'), 'YYYY/MM/DD'),
            TRY_TO_DATE(REGEXP_REPLACE(LEFT(TRIM(fecha_consumo::TEXT), 10), '[-]', '/'), 'DD/MM/YYYY'),
            TRY_TO_DATE(REGEXP_REPLACE(LEFT(TRIM(fecha_consumo::TEXT), 10), '[-]', '/'), 'MM/DD/YYYY')
        ) AS fecha_consumo,

        TRY_TO_DECIMAL(REGEXP_REPLACE(cantidad::TEXT, '[^0-9.]', ''), 10, 2) AS cantidad,

        TRY_TO_DECIMAL(REGEXP_REPLACE(subtotal::TEXT, '[^0-9.]', ''), 10, 2) AS subtotal,

        CURRENT_TIMESTAMP() AS _dbt_loaded_at
    
    FROM quitar_duplicados
    WHERE row_num = 1    
)

SELECT * FROM transformacion