{{ config(
    materialized='view'
)}}

WITH src_camas AS (
    SELECT
        id_habitacion::INTEGER AS id_habitacion,
        LISTAGG(UPPER(TRIM(tipo_cama::TEXT)), ' + ') WITHIN GROUP (ORDER BY tipo_cama) AS detalle_camas,
        COUNT(*) AS cantidad_camas
    FROM {{ source('hotel_raw', 'RAW_CAMA') }}
    WHERE UPPER(id_cama::TEXT) NOT LIKE '%ID_CAMA%'
    GROUP BY 1
),

src_habitacion AS (
    SELECT
        id_habitacion::INTEGER AS id_habitacion,
        id_hotel::INTEGER AS id_hotel,
        TRIM(numero_habitacion::TEXT) AS numero_habitacion,
        UPPER(TRIM(tipo::TEXT)) AS tipo_habitacion,
        UPPER(TRIM(estado::TEXT)) AS estado_habitacion,
        TRY_TO_DECIMAL(REGEXP_REPLACE(precio_noche::TEXT, '[^0-9.]', ''), 10, 2)::INTEGER AS precio_noche,
        capacidad_maxima::INTEGER AS capacidad_maxima,

        CASE
            WHEN TRIM(descripcion::text) = ''
                OR UPPER(TRIM(descripcion::TEXT)) IN ('N/A', 'NULL')
                OR descripcion IS NULL
            THEN 'Sin descripcion disponible'
            ELSE TRIM(descripcion::TEXT)
        END AS descripcion
        
    FROM {{ source('hotel_raw', 'RAW_HABITACION') }}
    WHERE UPPER(id_habitacion::TEXT) NOT LIKE '%ID_HABITACION%'
)

SELECT
    h.id_habitacion,
    h.id_hotel,
    h.numero_habitacion,
    h.tipo_habitacion,
    h.estado_habitacion,
    h.precio_noche,
    h.capacidad_maxima,
    h.descripcion,
    COALESCE(c.detalle_camas, 'Sin camas asignadas') AS detalle_camas,
    COALESCE(c.cantidad_camas, 0) AS cantidad_camas,
    CURRENT_TIMESTAMP() AS _dbt_loaded_at
FROM src_habitacion AS h
LEFT JOIN src_camas AS c ON h.id_habitacion = c.id_habitacion
WHERE h.id_habitacion IS NOT NULL