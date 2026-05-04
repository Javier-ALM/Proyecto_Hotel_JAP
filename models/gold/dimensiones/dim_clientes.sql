{{ config(
    materialized='table',
    schema='dimensiones'
) }}
with stg_clientes as (
    select * from {{ ref('silver_hotel_stg__clientes') }}
),
paises as (
    select * from {{ ref('paises_iso') }}
),
paises_normalizados as (
    select
        trim(upper(adjetivo)) as adjetivo_norm,
        trim(upper(iso_alfa_2)) as iso2_norm,
        trim(upper(nombre_pais)) as nombre_norm,
        trim(upper(iso_alfa_3)) as iso3_norm,
        nombre_pais,
        iso_alfa_2,
        iso_alfa_3
    from paises
),
clientes_con_pais as (
    select
        c.*,
        trim(upper(c.nacionalidad)) as nacionalidad_norm,
        p.nombre_pais,
        p.iso_alfa_2,
        p.iso_alfa_3,
        row_number() over (
            partition by c.id_cliente 
            order by 
                case 
                    when trim(upper(c.nacionalidad)) = p.adjetivo_norm then 1  -- Prioridad a adjetivo
                    when trim(upper(c.nacionalidad)) = p.iso2_norm then 2      -- Luego ISO2
                    when trim(upper(c.nacionalidad)) = p.iso3_norm then 3      -- Luego ISO3
                    when trim(upper(c.nacionalidad)) = p.nombre_norm then 4    -- Finalmente nombre
                    else 5
                end
        ) as rn
    from stg_clientes c
    left join paises_normalizados p 
        on c.nacionalidad is not null
        and trim(upper(c.nacionalidad)) in (p.adjetivo_norm, p.iso2_norm, p.iso3_norm, p.nombre_norm)
),
final as (
    select
        id_cliente,
        nombre as nombre_completo,
        dni_pasaporte,
        email,
        telefono,
        direccion,
        fecha_registro,
        datediff('day', fecha_registro, current_date()) as dias_desde_registro,
        coalesce(nombre_pais, nacionalidad) as nacionalidad,
        iso_alfa_2 as codigo_pais_iso2,
        iso_alfa_3 as codigo_pais_iso3,
        current_timestamp() as _dbt_updated_at
    from clientes_con_pais
    where rn = 1
)
select * from final