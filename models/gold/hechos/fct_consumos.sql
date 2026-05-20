{{ config(
    materialized='table',
    schema='hechos'
) }}

WITH consumos_transformados AS (
    SELECT
        id_consumo,
        id_reserva,
        id_servicio,
        fecha_consumo,
        cantidad,
        importe_consumo,
        _dbt_updated_at
    FROM {{ ref('silver_hotel_stg__consumo') }}
)

SELECT
    c.id_consumo,
    c.id_reserva,
    c.id_servicio,
    c.fecha_consumo,
    c.cantidad,
    c.importe_consumo,
    c._dbt_updated_at AS _dbt_updated_at
FROM consumos_transformados c
WHERE c.id_reserva IN (
    SELECT id_reserva 
    FROM {{ ref('fct_reservas') }}
)