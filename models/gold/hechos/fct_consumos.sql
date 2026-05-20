{{ config(
    materialized='table',
    schema='hechos',
    contract={'enforced': true}
) }}

SELECT
    c.id_consumo::INTEGER AS id_consumo,
    c.id_reserva::INTEGER AS id_reserva,
    c.id_servicio::INTEGER AS id_servicio,
    c.fecha_consumo::DATE AS fecha_consumo,
    c.cantidad::NUMBER AS cantidad,
    c.importe_consumo::NUMBER AS importe_consumo,
    c._dbt_updated_at::TIMESTAMP_LTZ AS _dbt_updated_at
FROM {{ ref('silver_hotel_stg__consumo') }} c
-- 🛡️ Filtro de integridad: Solo consumos ligados a reservas válidas
WHERE c.id_reserva IN (SELECT id_reserva FROM {{ ref('fct_reservas') }})