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
    
    {% if is_incremental() %}
      -- Solo traemos lo nuevo que NO esté ya en Gold
      WHERE id_reserva NOT IN (SELECT id_reserva FROM {{ this }})
    {% endif %}
),

habitaciones_dim AS (
    SELECT id_habitacion, id_hotel FROM {{ ref('dim_habitaciones') }}
)

SELECT
    r.id_reserva,
    r.id_cliente,
    h.id_hotel,
    r.id_habitacion,
    r.canal_reserva,
    r.fecha_checkin,
    r.fecha_checkout,
    r.numero_huespedes,
    r.estado_reserva,
    r.noches_estancia,
    r._dbt_loaded_at AS _dbt_inserted_at,
    r._dbt_loaded_at AS _dbt_updated_at
FROM reservas_clean r
LEFT JOIN habitaciones_dim h ON r.id_habitacion = h.id_habitacion