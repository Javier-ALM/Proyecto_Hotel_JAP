{{ config(
    materialized='table',
    schema='hechos',
    contract={'enforced': true}
) }}

SELECT
    id_consumo::INTEGER AS id_consumo,
    id_reserva::INTEGER AS id_reserva,
    id_servicio::INTEGER AS id_servicio,
    fecha_consumo::DATE AS fecha_consumo,
    cantidad::DECIMAL(10,2) AS cantidad,
    subtotal::DECIMAL(10,2) AS importe_consumo,
    _dbt_loaded_at::TIMESTAMP_LTZ AS _dbt_updated_at
FROM {{ ref('silver_hotel_stg__consumo') }}