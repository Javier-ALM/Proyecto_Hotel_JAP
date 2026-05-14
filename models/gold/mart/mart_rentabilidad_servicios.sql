{{ config(materialized='table') }}

WITH consumos AS (
    SELECT 
        id_servicio,
        id_reserva,
        importe_consumo
    FROM {{ ref('fct_consumos') }}
),

servicios AS (
    SELECT 
        id_servicio,
        nombre_servicio,
        categoria AS categoria_servicio
    FROM {{ ref('dim_servicio') }}
),

-- Añadimos el contexto del hotel desde la reserva
reservas AS (
    SELECT 
        r.id_reserva,
        h.nombre_hotel,
        h.ciudad,
        h.estrellas
    FROM {{ ref('fct_reservas') }} r
    JOIN {{ ref('dim_hoteles') }} h ON r.id_hotel = h.id_hotel
)

SELECT
    r.nombre_hotel,
    r.ciudad,
    r.estrellas,
    s.nombre_servicio,
    s.categoria_servicio,
    COUNT(c.id_reserva) AS veces_consumido,
    SUM(c.importe_consumo) AS ingresos_totales,
    ROUND(AVG(c.importe_consumo), 2) AS ticket_promedio
FROM consumos c
JOIN servicios s ON c.id_servicio = s.id_servicio
JOIN reservas r ON c.id_reserva = r.id_reserva
GROUP BY 1, 2, 3, 4, 5