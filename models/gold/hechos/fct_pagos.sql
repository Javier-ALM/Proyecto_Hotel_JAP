{{ config(
    materialized='table',
    schema='hechos',
    contract={'enforced': true}
) }}

SELECT
    id_pago::INTEGER AS id_pago,
    id_reserva::INTEGER AS id_reserva,
    fecha_pago::DATE AS fecha_pago,
    monto_total::NUMBER AS monto_total,
    forma_pago::VARCHAR AS forma_pago,
    estado_pago::VARCHAR AS estado_pago,
    referencia_pago::VARCHAR AS referencia_pago,
    -- Aquí usamos la columna que SÍ existe en tu tabla Silver:
    _dbt_loaded_at::TIMESTAMP_LTZ AS _dbt_updated_at
FROM {{ ref('silver_hotel_stg__pago') }}
-- 🛡️ Filtro de integridad
WHERE id_reserva IN (SELECT id_reserva FROM {{ ref('fct_reservas') }})