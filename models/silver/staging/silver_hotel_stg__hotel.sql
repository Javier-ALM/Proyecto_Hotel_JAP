{{ config(
    materialized='view'
) }}

WITH src_hotel AS (
    SELECT * FROM {{ source('hotel_raw', 'RAW_HOTEL') }}
),

-- 1. Traemos la referencia de los países desde tu seed
paises_ref AS (
    SELECT * FROM {{ ref('paises_iso') }}
),

transformacion AS (
    SELECT
        h.id_hotel,
        TRIM(h.nombre) AS nombre,
        COALESCE(
            CASE 
                WHEN UPPER(TRIM(h.direccion)) IN ('NULL', 'N/A', '', 'NONE', 'UNDEFINED') THEN NULL
                ELSE TRIM(h.direccion)
            END,
            'Sin direccion'
        ) AS direccion,
        TRIM(h.ciudad) AS ciudad,
        
        -- 2. CAMBIO CLAVE: Buscamos el código ISO en el seed. 
        -- Si no lo encuentra en el seed, dejamos el original para poder identificar el error.
        COALESCE(p.iso_alfa_3, UPPER(TRIM(h.pais))) AS pais,

        '+' || REGEXP_REPLACE(h.telefono, '[^0-9]', '') AS telefono,
        LOWER(TRIM(REGEXP_REPLACE(REGEXP_REPLACE(h.email, '_[0-9]+@', '@'), '[0-9]+\\.', '.'))) AS email,
        
        COALESCE(
            TRY_TO_DATE(LEFT(h.fecha_creacion, 10)),
            TRY_TO_DATE(h.fecha_creacion, 'YYYY/MM/DD'),
            TRY_TO_DATE(h.fecha_creacion, 'DD/MM/YYYY'),
            CURRENT_DATE()
        ) AS fecha_creacion,

        COALESCE(
            CASE
                WHEN UPPER(TRIM(h.activo)) IN ('1', 'TRUE', 'T', 'YES', 'Y') THEN TRUE
                WHEN UPPER(TRIM(h.activo)) IN ('0', 'FALSE', 'F', 'NO', 'N', '', 'NULL') THEN FALSE
                ELSE NULL
            END,
            FALSE
        ) AS activo,

        CASE 
            WHEN CAST(REGEXP_REPLACE(TRIM(h.categoria), '[^0-9]', '') AS INTEGER) >= 10 
            THEN CAST(REGEXP_REPLACE(TRIM(h.categoria), '[^0-9]', '') AS INTEGER) / 10
            ELSE CAST(REGEXP_REPLACE(TRIM(h.categoria), '[^0-9]', '') AS INTEGER)
        END::INTEGER AS categoria,
        
        CURRENT_TIMESTAMP() AS _dbt_loaded_at
    FROM src_hotel h
    LEFT JOIN paises_ref p 
        ON (
            CASE 
                WHEN UPPER(TRIM(h.pais)) = 'EAU' THEN 'EMIRATOS ÁRABES UNIDOS'
                ELSE UPPER(TRIM(h.pais))
            END
        ) = UPPER(TRIM(p.nombre_pais))
)

SELECT * FROM transformacion