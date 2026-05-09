{{
    config(
        materialized='view'
    )
}}

with src_reserva as (
    select
        id_reserva::integer as id_reserva,
        id_cliente::integer as id_cliente,
        id_habitacion::integer as id_habitacion,
        upper(trim(canal_reserva::text)) as canal_reserva,
        left(trim(fecha_checkin::text), 10) as fecha_checkin_raw,
        left(trim(fecha_checkout::text), 10) as fecha_checkout_raw,
        numero_huespedes::integer as numero_huespedes,
        upper(trim(estado_reserva::text)) as estado_reserva,
        case
            when trim(notas::text) = ''
                or upper(trim(notas::text)) in ('NAM', 'NULL', 'N/A', 'NONE')
                or notas is null
            then 'Sin notas'
            else trim(notas::text)
        end as notas
    from {{ source('hotel_raw', 'RAW_RESERVA') }}
    where id_reserva is not null
),

fechas_parseadas as (
    select
        id_reserva,
        id_cliente,
        id_habitacion,
        canal_reserva,
        numero_huespedes,
        estado_reserva,
        notas,
        coalesce(
            try_to_date(fecha_checkin_raw, 'YYYY-MM-DD'),
            try_to_date(fecha_checkin_raw, 'YYYY/MM/DD'),
            try_to_date(fecha_checkin_raw, 'DD/MM/YYYY'),
            try_to_date(fecha_checkin_raw, 'MM/DD/YYYY'),
            try_to_date(regexp_replace(fecha_checkin_raw, '[-]', '/'), 'YYYY/MM/DD')
        ) as fecha_checkin,
        coalesce(
            try_to_date(fecha_checkout_raw, 'YYYY-MM-DD'),
            try_to_date(fecha_checkout_raw, 'YYYY/MM/DD'),
            try_to_date(fecha_checkout_raw, 'DD/MM/YYYY'),
            try_to_date(fecha_checkout_raw, 'MM/DD/YYYY'),
            try_to_date(regexp_replace(fecha_checkout_raw, '[-]', '/'), 'YYYY/MM/DD')
        ) as fecha_checkout
    from src_reserva
),

fechas_corregidas as (
    select
        id_reserva,
        id_cliente,
        id_habitacion,
        canal_reserva,
        numero_huespedes,
        estado_reserva,
        notas,
        fecha_checkin,
        case
            when fecha_checkin is not null and fecha_checkout is null
                then dateadd(day, 1, fecha_checkin)
            when fecha_checkin is not null and fecha_checkout <= fecha_checkin
                then dateadd(day, 1, fecha_checkin)
            else fecha_checkout
        end as fecha_checkout
    from fechas_parseadas
)

select
    id_reserva,
    id_cliente,
    id_habitacion,
    canal_reserva,
    fecha_checkin,
    fecha_checkout,
    numero_huespedes,
    estado_reserva,
    notas,
    datediff(day, fecha_checkin, fecha_checkout) as noches_estancia,
    current_timestamp() as _dbt_loaded_at
from fechas_corregidas
where id_habitacion in (
    select id_habitacion
    from {{ ref('silver_hotel_stg__habitacion') }}
)
and fecha_checkin is not null
and fecha_checkout is not null
qualify row_number() over (
    partition by id_reserva
    order by fecha_checkin desc, fecha_checkout desc, id_habitacion desc
) = 1