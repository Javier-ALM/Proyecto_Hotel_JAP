{{ config(materialized='table') }}

WITH calendario AS (
    SELECT fecha 
    FROM {{ ref('dim_tiempo') }}
    WHERE fecha >= '2024-01-01'
),

reservas_expandidas AS (
    SELECT 
        c.fecha,
        r.id_hotel,
        r.id_reserva,
        r.id_habitacion
    FROM {{ ref('fct_reservas') }} r
    JOIN calendario c 
        ON c.fecha >= r.fecha_checkin 
        AND c.fecha < r.fecha_checkout
    -- FILTRO BLINDADO: Solo estados que ocupan habitación física
    WHERE r.estado_reserva IN ('CONFIRMADA', 'FINALIZADA', 'PENDIENTE')
),

capacidad_hotel AS (
    SELECT 
        id_hotel,
        COUNT(id_habitacion) AS total_habitaciones_disponibles
    FROM {{ ref('dim_habitaciones') }}
    GROUP BY 1
),

info_hoteles AS (
    SELECT id_hotel, nombre_hotel, ciudad, estrellas
    FROM {{ ref('dim_hoteles') }}
)

SELECT
    re.fecha,
    h.nombre_hotel,
    h.ciudad,
    h.estrellas,
    COUNT(DISTINCT re.id_reserva) AS habitaciones_ocupadas,
    MAX(cap.total_habitaciones_disponibles) AS capacidad_total,
    ROUND(
        (COUNT(DISTINCT re.id_reserva) / NULLIF(MAX(cap.total_habitaciones_disponibles), 0)) * 100, 
        2
    ) AS porcentaje_ocupacion

FROM reservas_expandidas re
JOIN info_hoteles h ON re.id_hotel = h.id_hotel
JOIN capacidad_hotel cap ON re.id_hotel = cap.id_hotel
GROUP BY 1, 2, 3, 4