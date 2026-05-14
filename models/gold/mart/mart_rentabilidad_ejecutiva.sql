{{ config(materialized='table') }}

WITH reservas AS (
    SELECT 
        id_reserva,
        id_hotel,
        id_habitacion,
        canal_reserva,
        fecha_checkin,
        noches_estancia,
        estado_reserva
    FROM {{ ref('fct_reservas') }} 
    WHERE estado_reserva NOT IN ('CANCELADA', 'NO_PRESENTADO')
),

-- 1. Lógica de Costes (Simulada para el análisis)
habitaciones_con_coste AS (
    SELECT 
        id_habitacion,
        id_hotel,
        tipo_habitacion,
        precio_noche,
        CASE 
            WHEN (precio_noche * 0.4) < 30 THEN 30
            ELSE ROUND(precio_noche * 0.4, 2)
        END AS coste_noche_estimado
    FROM {{ ref('dim_habitaciones') }}
),

-- 2. Ingresos por Alojamiento
ingresos_alojamiento AS (
    SELECT 
        id_reserva,
        SUM(monto_total) AS total_pagado
    FROM {{ ref('fct_pagos') }}
    WHERE estado_pago = 'COMPLETADO'
    GROUP BY 1
),

-- 3. Ingresos por Extras
ingresos_extras AS (
    SELECT 
        id_reserva,
        SUM(importe_consumo) AS total_extras
    FROM {{ ref('fct_consumos') }}
    GROUP BY 1
),

-- 4. Información de Hoteles
info_hoteles AS (
    SELECT 
        id_hotel,
        nombre_hotel,
        ciudad,
        estrellas
    FROM {{ ref('dim_hoteles') }}
)

SELECT
    h.nombre_hotel,
    h.estrellas,
    h.ciudad,
    r.canal_reserva,
    DATE_TRUNC('month', r.fecha_checkin) AS mes_operativo,
    
    COUNT(DISTINCT r.id_reserva) AS total_reservas,
    SUM(r.noches_estancia) AS total_noches,

    SUM(COALESCE(a.total_pagado, 0)) AS ingresos_alojamiento,
    SUM(COALESCE(e.total_extras, 0)) AS ingresos_extras,
    SUM(COALESCE(a.total_pagado, 0) + COALESCE(e.total_extras, 0)) AS ingresos_totales,

    SUM(r.noches_estancia * hab.coste_noche_estimado) AS costes_operativos_estimados,
    
    (SUM(COALESCE(a.total_pagado, 0) + COALESCE(e.total_extras, 0))) - 
    (SUM(r.noches_estancia * hab.coste_noche_estimado)) AS beneficio_neto,

    ROUND(
        ( (SUM(COALESCE(a.total_pagado, 0) + COALESCE(e.total_extras, 0))) - 
          (SUM(r.noches_estancia * hab.coste_noche_estimado)) ) / 
        NULLIF(SUM(COALESCE(a.total_pagado, 0) + COALESCE(e.total_extras, 0)), 0) * 100, 2
    ) AS margen_lucro_pct

FROM reservas r
JOIN habitaciones_con_coste hab ON r.id_habitacion = hab.id_habitacion
INNER JOIN info_hoteles h ON r.id_hotel = h.id_hotel
LEFT JOIN ingresos_alojamiento a ON r.id_reserva = a.id_reserva
LEFT JOIN ingresos_extras e ON r.id_reserva = e.id_reserva

GROUP BY 1, 2, 3, 4, 5
-- Filtro para asegurar que solo vemos registros con ingresos reales
HAVING ingresos_totales > 0