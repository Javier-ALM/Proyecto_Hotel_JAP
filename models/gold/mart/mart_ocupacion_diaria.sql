{{ config(materialized='table') }}

WITH fact_reservas AS (
    SELECT * FROM {{ ref('fct_reservas') }}
),
dim_habitaciones AS (
    SELECT * FROM {{ ref('dim_habitaciones') }} -- Nombre correcto del modelo
)

SELECT
    r.fecha_checkin AS fecha,
    h.id_hotel,
    h.nombre_hotel,
    COUNT(r.id_reserva) AS total_reservas_activas,
    SUM(h.precio_noche) AS ingresos_estimados_noche,
    ROUND(AVG(h.precio_noche), 2) AS adr_promedio 
FROM fact_reservas r
-- Usamos el alias 'h' que apunta a la CTE 'dim_habitaciones'
JOIN dim_habitaciones h ON r.id_habitacion = h.id_habitacion 
WHERE r.estado_reserva NOT IN ('CANCELADA')
GROUP BY 1, 2, 3