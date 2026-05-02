{{ config(
    materialized='view'
)}}

WITH src_servicio AS (
    SELECT * FROM {{ source('hotel_raw', 'RAW_SERVIVIO') }}
),

transformacion AS (
    SELECT
        TRIM(id_servicio) AS id_servicio,
        TRIM(nombre_servicio) AS nombre_servicio,
        UPPER(TRIM(categoria)) AS categoria,
        COALESCE(
            NULLIF(TRIM(descripcion), ''),
            'Sin descripcion'
        ) AS descripcion,
        CAST(REGEXP_REPLACE(TRIM(precio_unitario), '[^0-9.]', '') AS DECIMAL(10,2)) AS precio_unitario,
        CASE
            WHEN UPPER(TRIM(activo)) IN ('1', 'TRUE', 'T', 'YES', 'Y', 'ACTIVO') THEN TRUE
            WHEN UPPER(TRIM(activo)) IN ('0', 'FALSE', 'F', 'NO', 'N', '', 'INACTIVO') THEN FALSE
            ELSE NULL
        END AS activo,
        CURRENT_TIMESTAMP() AS _dbt_loaded_at
    FROM src_servicio
)
SELECT * FROM transformacion