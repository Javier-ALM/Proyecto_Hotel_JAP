{{ config(
    materialized='table',
    schema='hechos',
    contract={'enforced': true}
) }}

SELECT
    id_consumo, -- Ya es INTEGER en Silver
    id_reserva, -- Ya es INTEGER en Silver
    id_servicio, -- Ya es INTEGER en Silver
    fecha_consumo, -- Ya es DATE en Silver
    cantidad, -- Ya es DECIMAL en Silver
    importe_consumo, -- 🚨 AQUÍ: En Silver ya se llama así, no uses 'subtotal'
    _dbt_updated_at -- 🚨 AQUÍ: Silver genera '_dbt_updated_at', no '_dbt_loaded_at'
FROM {{ ref('silver_hotel_stg__consumo') }}