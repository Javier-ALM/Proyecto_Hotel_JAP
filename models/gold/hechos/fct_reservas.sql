{{ config(
    materialized='incremental',
    unique_key='id_reserva',
    incremental_strategy='delete+insert',
    on_schema_change='append_new_columns',
    schema='hechos',
    contract={'enforced': true}
) }}

WITH reservas_clean AS (
    SELECT * FROM {{ ref('silver_hotel_stg__reserva') }}
    
    -- 🛡️ FILTRO DE INTEGRIDAD: Solo reservas con clientes existentes
    WHERE id_cliente IN (SELECT id_cliente FROM {{ ref('dim_clientes') }})
    
    {% if is_incremental() %}
      AND _dbt_loaded_at > (SELECT MAX(_dbt_updated_at) FROM {{ this }})
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
    r.canal_reserva::VARCHAR AS canal_reserva,
    r.fecha_checkin::DATE AS fecha_checkin,
    r.fecha_checkout::DATE AS fecha_checkout,
    r.numero_huespedes::INTEGER AS numero_huespedes,
    r.estado_reserva::VARCHAR AS estado_reserva,
    r.noches_estancia::INTEGER AS noches_estancia,
    r._dbt_loaded_at::TIMESTAMP_LTZ AS _dbt_inserted_at,
    CURRENT_TIMESTAMP()::TIMESTAMP_LTZ AS _dbt_updated_at
FROM reservas_clean r
LEFT JOIN habitaciones_dim h ON r.id_habitacion = h.id_habitacion