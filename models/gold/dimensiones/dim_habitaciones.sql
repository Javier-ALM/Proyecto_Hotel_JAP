{{ config(
    materialized='table',
    schema='dimensiones'
) }}

with stg_habitaciones as (
    -- Referencia correcta al modelo Silver (singular)
    select * from {{ ref('silver_hotel_stg__habitacion') }}
),

dim_hoteles as (
    select id_hotel, nombre_hotel from {{ ref('dim_hoteles') }}
),

final as (
    select
        h.id_habitacion,
        h.id_hotel,
        dh.nombre_hotel,
        h.numero_habitacion,
        h.tipo_habitacion,
        h.estado_habitacion,
        h.precio_noche,
        h.capacidad_maxima,
        h.descripcion,
        h.detalle_camas,
        h.cantidad_camas,
        -- Regla de negocio: Categoría de precio
        case 
            when h.precio_noche > 250 then 'PREMIUM'
            when h.precio_noche < 150 then 'ECONOMICO'
            else 'ESTANDAR'
        end as categoria_precio,
        current_timestamp() as _dbt_updated_at
    from stg_habitaciones h
    left join dim_hoteles dh on h.id_hotel = dh.id_hotel
)

select * from final