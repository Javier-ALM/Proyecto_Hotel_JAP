{{ config(
    materialized='incremental',
    incremental_strategy='append',
    on_schema_change='append_new_columns',
    schema='hechos',
    contract={'enforced': true}
) }}

WITH reservas_source AS (
    SELECT * FROM {{ ref('silver_hotel_stg__reserva') }}
    
    {% if is_incremental() %}
      WHERE _dbt_loaded_at > (SELECT COALESCE(MAX(_dbt_updated_at), '1900-01-01'::TIMESTAMP_LTZ) FROM {{ this }})
    {% endif %}
),

habitaciones_dim AS (
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
FROM reservas_source r
LEFT JOIN habitaciones_dim h 
    ON r.id_habitacion = h.id_habitacion