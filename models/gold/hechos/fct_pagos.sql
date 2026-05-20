{{ config(
    materialized='table',
    schema='hechos'
) }}

WITH pagos_transformados AS (
    SELECT
        id_pago,
        id_reserva,
        fecha_pago,
        monto_total,
        forma_pago,
        estado_pago,
        referencia_pago,
        _dbt_loaded_at
    FROM {{ ref('silver_hotel_stg__pago') }}
)

SELECT
    p.id_pago,
    p.id_reserva,
    p.fecha_pago,
    p.monto_total,                         -- Nombre exacto exigido por el contrato estructurado
    p.forma_pago,
    p.estado_pago,
    p.referencia_pago,
    p._dbt_loaded_at AS _dbt_updated_at    -- Sincronizado con el campo de auditoría del contrato
FROM pagos_transformados p
-- 🛡️ AQUÍ SE HACE EL FILTRADO: Protege las relaciones del Modelo Estrella
WHERE p.id_reserva IN (
    SELECT id_reserva 
    FROM {{ ref('fct_reservas') }}
)