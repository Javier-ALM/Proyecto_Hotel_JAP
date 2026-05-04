{{
    config(
        materialized='view'
    )
}}

WITH src_reserva AS (
    SELECT
        id_reserva::INTEGER AS id_reserva,
        id_cliente::INTEGER AS id_cliente,
        id_habitacion::INTEGER AS id_habitacion,
        UPPER(Trim(canal_reserva::TEXT)) AS canal_reserva,
        REGEXP_REPLACE(LEFT(TRIM(fecha_checkin::TEXT), 10), '[-]', '/') AS fecha_checkin,
        REGEXP_REPLACE(LEFT(TRIM(fecha_checkout::TEXT), 10), '[-]', '/') AS fecha_checkout,
        numero_huespedes::INTEGER AS numero_huespedes,
        UPPER(TRIM(estado_reserva::TEXT)) AS estado_reserva,
        CASE
            WHEN TRIM(notas::TEXT) = ''
                OR UPPER(TRIM(notas::TEXT)) IN ('NAM', 'NULL', 'N/A', 'NONE')
                OR notas IS NULL
            THEN 'Sin notas'
            ELSE TRIM(notas::TEXT)
        END AS notas
    FROM {{ source('hotel_raw', 'RAW_RESERVA') }}
    WHERE id_reserva IS NOT NULL
),

final_fechas AS (
    SELECT
        id_reserva,
        id_cliente,
        id_habitacion,
        canal_reserva,
        numero_huespedes,
        estado_reserva,
        notas,
        COALESCE(
            TRY_TO_DATE(fecha_checkin, 'YYYY/MM/DD'),
            TRY_TO_DATE(fecha_checkin, 'DD/MM/YYYY'),
            TRY_TO_DATE(fecha_checkin, 'MM/DD/YYYY')
        ) AS fecha_checkin,
        COALESCE(
            TRY_TO_DATE(fecha_checkout, 'YYYY/MM/DD'),
            TRY_TO_DATE(fecha_checkout, 'DD/MM/YYYY'),
            TRY_TO_DATE(fecha_checkout, 'MM/DD/YYYY')
        ) AS fecha_checkout
    FROM src_reserva
)

SELECT 
    id_reserva,
    id_cliente,
    id_habitacion,
    canal_reserva,
    fecha_checkin,
    fecha_checkout,
    numero_huespedes,
    estado_reserva,
    notas,
    CASE 
        WHEN fecha_checkin IS NOT NULL 
            AND fecha_checkout IS NOT NULL
            AND fecha_checkout > fecha_checkin 
        THEN DATEDIFF(day, fecha_checkin, fecha_checkout) 
        ELSE NULL 
    END AS noches_estancia,
    CURRENT_TIMESTAMP() AS _dbt_loaded_at
FROM final_fechas