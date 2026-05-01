{{ config(
    materialized='view'
)}}

with src_habitacion AS (
    SELECT * FROM {{ source('hotel_raw', 'RAW_HABITACION') }}
),

transformacion AS (
    SELECT
        CAST(ID_HABITACION AS INTEGER) AS id_habitacion,
        CAST(ID_HOTEL AS INTEGER) AS id_hotel,
        TRIM(NUMERO_HABITACION) AS numero_habitacion,
        UPPER(TRIM(TIPO)) AS tipo_habitacion,
        UPPER(TRIM(ESTADO)) AS estado_habitacion,
        CAST(REGEXP_REPLACE(PRECIO_NOCHE, '[^0-9.]', '') AS DECIMAL(10,2)) As precio_noche,
        CAST(CAPACIDAD_MAXIMA AS INTEGER) AS capacidad_maxima,
        NULLIF(TRIM(DESCRIPCION), '') AS descripcion,
        CURRENT_TIMESTAMP() AS _dbt_loaded_at
    FROM src_habitacion
)

SELECT * FROM transformacion