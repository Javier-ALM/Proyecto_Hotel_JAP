{{ config(
    materialized='table',
    schema='hechos',
    contract={'enforced': true}
) }}

WITH reservas AS (
    SELECT * FROM {{ ref('silver_hotel_stg__reserva') }}
),

habitaciones AS (
    SELECT id_habitacion, id_hotel FROM {{ ref('dim_habitaciones') }}
)

SELECT
    r.id_reserva::INTEGER AS id_reserva,
    r.id_cliente::INTEGER AS id_cliente,
    h.id_hotel::INTEGER AS id_hotel,
    r.id_habitacion::INTEGER AS id_habitacion,
    r.canal_reserva::TEXT AS canal_reserva,
    r.fecha_checkin::DATE AS fecha_checkin,
    r.fecha_checkout::DATE AS fecha_checkout,
    r.numero_huespedes::INTEGER AS numero_huespedes,
    r.estado_reserva::TEXT AS estado_reserva,
    r.noches_estancia::INTEGER AS noches_estancia,
    r._dbt_loaded_at::TIMESTAMP_LTZ AS _dbt_updated_at
FROM reservas r
LEFT JOIN habitaciones h 
    ON r.id_habitacion = h.id_habitacion