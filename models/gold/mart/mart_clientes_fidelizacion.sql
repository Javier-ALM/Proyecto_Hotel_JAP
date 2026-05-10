{{ config(materialized='table') }}

WITH clientes AS (
    SELECT id_cliente, nombre_completo, nacionalidad 
    FROM {{ ref('dim_clientes') }}
),

reservas_agg AS (
    SELECT 
        id_cliente, 
        COUNT(*) AS total_estancias
    FROM {{ ref('fct_reservas') }}
    WHERE estado_reserva = 'FINALIZADA'
    GROUP BY 1
),

pagos_agg AS (
    -- Importante: Nombramos la columna de suma de forma distinta a la CTE
    SELECT 
        r.id_cliente, 
        SUM(p.monto_total) AS ltv_historico
    FROM {{ ref('fct_pagos') }} p
    JOIN {{ ref('fct_reservas') }} r ON p.id_reserva = r.id_reserva
    GROUP BY 1
)

SELECT
    c.id_cliente,
    c.nombre_completo,
    c.nacionalidad,
    COALESCE(ra.total_estancias, 0) AS num_estancias,
    COALESCE(pa.ltv_historico, 0) AS total_invertido,
    -- Usamos la columna final calculada para la segmentación
    CASE 
        WHEN COALESCE(pa.ltv_historico, 0) >= 5000 THEN 'VIP'
        WHEN COALESCE(pa.ltv_historico, 0) >= 2000 THEN 'Recurrente'
        ELSE 'Ocasional'
    END AS segmento_cliente
FROM clientes c
LEFT JOIN reservas_agg ra ON c.id_cliente = ra.id_cliente
LEFT JOIN pagos_agg pa ON c.id_cliente = pa.id_cliente