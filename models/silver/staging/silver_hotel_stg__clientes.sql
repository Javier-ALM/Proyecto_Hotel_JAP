{{ config(
    materialized='view'
)}}

with src_snapshot AS (
    SELECT * FROM {{ ref('snp_clientes') }}
),

-- 1. Capa intermedia de limpieza y tipado
limpieza as (
    SELECT
        "ID_CLIENTE" as id_cliente,
        TRIM("NOMBRE") as nombre,
        TRIM("DNI_PASAPORTE") as dni_pasaporte,
        TRIM("NACIONALIDAD") as nacionalidad,
        
        -- 🛡️ NORMALIZACIÓN DE TEXTO SUCIO: Convertimos cadenas 'NULL' y 'N/A' en verdaderos NULLs analíticos
        CASE 
            WHEN UPPER(TRIM("DIRECCION")) IN ('NULL', 'N/A', '', 'NONE', 'NAN') THEN NULL
            ELSE TRIM("DIRECCION")
        END as direccion,
        
        TRIM("EMAIL") as email,
        TRIM("TELEFONO") as telefono,
        "FECHA_REGISTRO" as fecha_registro,
        "DBT_UPDATED_AT" as dbt_updated_at,
        "DBT_VALID_FROM" as dbt_valid_from,
        CURRENT_TIMESTAMP() AS _dbt_loaded_at
    FROM src_snapshot
    WHERE "DBT_VALID_TO" IS NULL
)

-- 2. Filtro de exclusión final basado en datos limpios
SELECT * FROM limpieza
WHERE direccion IS NOT NULL