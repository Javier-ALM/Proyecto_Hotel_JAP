{{
    config(
        materialized='view'
    )
}}

with src_consumo as (
    select *
    from {{ source('hotel_raw', 'RAW_CONSUMO') }}
),

quitar_duplicados as (
    select
        *,
        row_number() over (partition by id_consumo order by id_consumo) as row_num
    from src_consumo
),

transformacion as (
    select
        id_consumo::integer as id_consumo,
        id_reserva::integer as id_reserva,
        trim(id_servicio::text)::integer as id_servicio,
        coalesce(
            try_to_date(regexp_replace(left(trim(fecha_consumo::text), 10), '[-]', '/'), 'YYYY/MM/DD'),
            try_to_date(regexp_replace(left(trim(fecha_consumo::text), 10), '[-]', '/'), 'YYYY-MM-DD'),
            try_to_date(regexp_replace(left(trim(fecha_consumo::text), 10), '[-]', '/'), 'DD/MM/YYYY'),
            try_to_date(regexp_replace(left(trim(fecha_consumo::text), 10), '[-]', '/'), 'MM/DD/YYYY')
        ) as fecha_consumo,
        try_to_decimal(regexp_replace(cantidad::text, '[^0-9.]', ''), 10, 2) as cantidad,
        try_to_decimal(regexp_replace(subtotal::text, '[^0-9.]', ''), 10, 2) as importe_consumo,
        current_timestamp() as _dbt_updated_at
    from quitar_duplicados
    where row_num = 1
)

-- 🛡️ Salida limpia directa: Sin subconsultas que rompan el Grafo del JOB
select
    id_consumo,
    id_reserva,
    id_servicio,
    fecha_consumo,
    cantidad,
    importe_consumo,
    _dbt_updated_at
from transformacion