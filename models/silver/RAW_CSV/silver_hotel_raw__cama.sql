{{ config(
    materialized='view'
)}}

WITH src_cama AS (
    SELECT * FROM {{ source('hotel_raw', 'RAW_CAMA')}}
),

transformacion AS (
    SELECT
        CAST(id_cama AS INTEGER) AS id_cama, 
        CAST(id_habitacion AS INTEGER) AS id_habitacion,
        UPPER(TRIM(tipo_cama)) AS tipo_cama,
        CURRENT_TIMESTAMP() AS _dbt_loaded_at
    FROM src_cama
)

SELECT * FROM transformacion