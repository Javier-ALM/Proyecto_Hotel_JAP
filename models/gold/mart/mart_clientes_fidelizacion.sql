{{ config(materialized='table') }}

WITH clientes AS (
    SELECT id_cliente, nombre_completo, nacionalidad 
    FROM {{ ref('dim_clientes') }}
),

reservas_agg AS (
    SELECT 
        id_cliente, 
        COUNT(*) AS total_estancias,
        MAX(fecha_checkout) AS ultima_estancia
    FROM {{ ref('fct_reservas') }}
    WHERE estado_reserva = 'FINALIZADA'
    GROUP BY 1
),

pagos_agg AS (
    SELECT 
        r.id_cliente, 
        SUM(p.monto_total) AS ltv_historico
    FROM {{ ref('fct_pagos') }} p
    JOIN {{ ref('fct_reservas') }} r ON p.id_reserva = r.id_reserva
    WHERE p.estado_pago = 'COMPLETADO'
    GROUP BY 1
)

SELECT
    c.id_cliente,
    c.nombre_completo,
    c.nacionalidad,
    COALESCE(ra.total_estancias, 0) AS num_estancias,
    COALESCE(pa.ltv_historico, 0) AS total_invertido,
    -- Calculamos los días desde la última vez que nos visitó hasta hoy
    COALESCE(DATEDIFF('day', ra.ultima_estancia, CURRENT_DATE()), 999) AS recencia_dias,
    
    -- Nueva Segmentación Avanzada
    CASE 
        WHEN COALESCE(pa.ltv_historico, 0) >= 5000 AND DATEDIFF('day', ra.ultima_estancia, CURRENT_DATE()) <= 365 THEN 'VIP Activo'
        WHEN COALESCE(pa.ltv_historico, 0) >= 5000 AND DATEDIFF('day', ra.ultima_estancia, CURRENT_DATE()) > 365 THEN 'VIP en Riesgo'
        WHEN COALESCE(pa.ltv_historico, 0) >= 2000 THEN 'Recurrente'
        ELSE 'Ocasional'
    END AS segmento_cliente

FROM clientes c
LEFT JOIN reservas_agg ra ON c.id_cliente = ra.id_cliente
LEFT JOIN pagos_agg pa ON c.id_cliente = pa.id_cliente