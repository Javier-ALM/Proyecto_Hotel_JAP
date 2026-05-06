{{ config(
    materialized='incremental',
    unique_key='id_reserva',
    incremental_strategy='merge',
    on_schema_change='append_new_columns',
    schema='hechos',
    contract={'enforced': true}
) }}

WITH reservas AS (
    SELECT * FROM {{ ref('silver_hotel_stg__reserva') }}
    
    {% if is_incremental() %}
      -- Filtro incremental: usamos COALESCE por si la tabla está vacía en la primera carga
      WHERE _dbt_loaded_at > (SELECT COALESCE(MAX(_dbt_updated_at), '1900-01-01') FROM {{ this }})
    {% endif %}
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

    CASE 
        WHEN r.fecha_checkout > r.fecha_checkin
        THEN DATEDIFF(DAY, r.fecha_checkin, r.fecha_checkout)
        ELSE 0
    END::INTEGER AS noches_estancia,
    
    r._dbt_loaded_at::TIMESTAMP_LTZ AS _dbt_updated_at
FROM reservas r
LEFT JOIN habitaciones h 
    ON r.id_habitacion = h.id_habitacion