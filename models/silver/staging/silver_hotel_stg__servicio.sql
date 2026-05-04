{{ config(
    materialized='view'
)}}

WITH limpieza_inicial AS (
    SELECT
        TRIM(id_servicio::TEXT)::INTEGER AS id_servicio,
        UPPER(TRIM(nombre_servicio::TEXT)) AS nombre_servicio,
        UPPER(TRIM(categoria::TEXT)) AS categoria,
        
        CASE
            WHEN TRIM(descripcion::TEXT) = '' OR descripcion IS NULL 
                 OR UPPER(TRIM(descripcion::TEXT)) IN ('NULL', 'N/A', 'NONE') 
            THEN 'Sin descripción disponible'
            ELSE TRIM(descripcion::TEXT)
        END AS descripcion,
        
        TRY_TO_DECIMAL(REGEXP_REPLACE(precio_unitario::TEXT, '[^0-9.]', ''), 10, 2)::INTEGER AS precio_unitario,
        
        CASE
            WHEN UPPER(TRIM(activo::TEXT)) IN ('1', 'TRUE', 'T', 'YES', 'Y', 'ACTIVO') THEN TRUE
            WHEN UPPER(TRIM(activo::TEXT)) IN ('0', 'FALSE', 'F', 'NO', 'N', 'INACTIVO') THEN FALSE
            ELSE TRUE 
        END AS es_activo

    FROM {{ source('hotel_raw', 'RAW_SERVICIO') }} 
    WHERE id_servicio IS NOT NULL
),

deduplicado AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (
            PARTITION BY nombre_servicio 
            ORDER BY id_servicio ASC
        ) AS row_num
    FROM limpieza_inicial
)

SELECT 
    id_servicio,
    nombre_servicio,
    categoria,
    descripcion,
    precio_unitario,
    es_activo,
    CURRENT_TIMESTAMP() AS _dbt_loaded_at
FROM deduplicado
WHERE row_num = 1