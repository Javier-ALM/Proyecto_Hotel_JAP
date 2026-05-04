{{ config(
    materialized='table',
    schema='dimensiones'
) }}

with stg_clientes as (
    select * from {{ ref('silver_hotel_stg__clientes') }}
),

paises as (
    select * from {{ ref('paises_iso') }} -- Tu seed con 4 columnas
),

final as (
    select
        c.id_cliente,
        c.nombre as nombre_completo,
        c.email,
        -- Obtenemos el nombre real y los códigos oficiales gracias al join por adjetivo
        coalesce(p.nombre_pais, c.nacionalidad) as nacionalidad,
        p.iso_alfa_2 as codigo_pais_iso2,
        p.iso_alfa_3 as codigo_pais_iso3
    from stg_clientes c
    left join paises p 
        on trim(upper(c.nacionalidad)) = trim(upper(p.adjetivo)) -- El pegamento es el adjetivo
)

select * from final