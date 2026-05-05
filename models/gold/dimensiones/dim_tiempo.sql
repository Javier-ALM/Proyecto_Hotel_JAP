{{ config(
    materialized='table',
    database='HOTEL_DEV_GOLD_DB',
    schema='dimensiones',
    config={
      "contract": {"enforced": true}
    }
) }}

WITH generador_filas AS (
    SELECT 
        ROW_NUMBER() OVER (ORDER BY seq4()) - 1 AS incremento
    FROM TABLE(GENERATOR(ROWCOUNT => 3650))
),

base_fechas AS (
    SELECT 
        DATEADD(day, incremento, '2024-01-01') AS fecha_raw
    FROM generador_filas
),

final AS (
    SELECT
        CAST(fecha_raw AS DATE) AS fecha,
        
        EXTRACT(year FROM fecha_raw) AS anio,
        EXTRACT(month FROM fecha_raw) AS mes,
        
        CASE EXTRACT(month FROM fecha_raw)
            WHEN 1 THEN 'Enero' WHEN 2 THEN 'Febrero' WHEN 3 THEN 'Marzo'
            WHEN 4 THEN 'Abril' WHEN 5 THEN 'Mayo' WHEN 6 THEN 'Junio'
            WHEN 7 THEN 'Julio' WHEN 8 THEN 'Agosto' WHEN 9 THEN 'Septiembre'
            WHEN 10 THEN 'Octubre' WHEN 11 THEN 'Noviembre' WHEN 12 THEN 'Diciembre'        
        END AS nombre_mes,
        
        EXTRACT(day FROM fecha_raw) AS dia,
        DAYOFWEEK(fecha_raw) AS dia_semana,
        
        CASE DAYOFWEEK(fecha_raw)
            WHEN 1 THEN 'Lunes' WHEN 2 THEN 'Martes' WHEN 3 THEN 'Miercoles'
            WHEN 4 THEN 'Jueves' WHEN 5 THEN 'Viernes' WHEN 6 THEN 'Sabado' 
            WHEN 0 THEN 'Domingo'
        END AS nombre_dia,
        
        EXTRACT(quarter FROM fecha_raw) AS trimestre,
        CASE WHEN DAYOFWEEK(fecha_raw) IN (6, 0) THEN TRUE ELSE FALSE END AS es_fin_de_semana
    FROM base_fechas
)

SELECT * FROM final