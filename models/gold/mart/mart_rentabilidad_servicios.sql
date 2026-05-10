{{ config(materialized='table') }}

SELECT
    s.nombre_servicio,
    s.categoria,
    COUNT(c.id_consumo) AS veces_consumido,
    SUM(c.importe_consumo) AS ingresos_totales_servicio,
    AVG(c.importe_consumo) AS ticket_promedio
FROM {{ ref('fct_consumos') }} AS c 
JOIN {{ ref('dim_servicio') }} AS s ON c.id_servicio = s.id_servicio
GROUP BY 1, 2