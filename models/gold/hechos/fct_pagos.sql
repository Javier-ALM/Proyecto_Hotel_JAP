{{ config(
    materialized='table',
    schema='hechos',
    contract={'enforced': true}
) }}

SELECT
    id_pago::INTEGER AS id_pago,
    id_reserva::INTEGER AS id_reserva,
    fecha_pago::DATE AS fecha_pago,
    monto_total::DECIMAL(10,2) AS monto_total,
    forma_pago::TEXT AS forma_pago,
    estado_pago::TEXT AS estado_pago,
    referencia_pago::TEXT AS referencia_pago,
    _dbt_loaded_at::TIMESTAMP_LTZ AS _dbt_updated_at
FROM {{ ref('silver_hotel_stg__pago') }}