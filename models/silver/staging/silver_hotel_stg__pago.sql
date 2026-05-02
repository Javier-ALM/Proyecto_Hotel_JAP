{{ config(
    materialized='view'
) }}

WITH src_pago AS (
    SELECT * FROM {{ source('hotel_raw', 'RAW_PAGO') }}
),

quitar_duplicados AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY id_pago ORDER BY id_pago) AS row_num
    FROM src_pago
),

transformacion AS (
    SELECT
        id_pago::INTEGER AS id_pago,
        id_reserva::INTEGER AS id_reserva,
        
        COALESCE(
            TRY_TO_DATE(REGEXP_REPLACE(LEFT(TRIM(fecha_pago::TEXT), 10), '[-]', '/'), 'YYYY/MM/DD'),
            TRY_TO_DATE(REGEXP_REPLACE(LEFT(TRIM(fecha_pago::TEXT), 10), '[-]', '/'), 'DD/MM/YYYY'),
            TRY_TO_DATE(REGEXP_REPLACE(LEFT(TRIM(fecha_pago::TEXT), 10), '[-]', '/'), 'MM/DD/YYYY')
        ) AS fecha_pago,

        TRY_TO_DECIMAL(REGEXP_REPLACE(monto_total::TEXT, '[^0-9.]', ''), 10, 2) AS monto_total,
        
        UPPER(TRIM(forma_pago::TEXT)) AS forma_pago,
        UPPER(TRIM(estado_pago::TEXT)) AS estado_pago,
        
        CASE 
            WHEN TRIM(referencia_pago::TEXT) = '' 
                 OR UPPER(TRIM(referencia_pago::TEXT)) IN ('NAN', 'NULL', 'N/A') 
                 OR referencia_pago IS NULL 
            THEN 'SIN REFERENCIA'
            ELSE TRIM(referencia_pago::TEXT)
        END AS referencia_pago,
        
        CURRENT_TIMESTAMP() AS _dbt_loaded_at
    FROM quitar_duplicados
    WHERE row_num = 1
)

SELECT * FROM transformacion