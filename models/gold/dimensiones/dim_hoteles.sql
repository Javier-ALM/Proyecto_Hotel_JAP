{{ config(
    materialized='table',
    schema='dimensiones'
) }}
with stg_hoteles as (
    select * from {{ ref('silver_hotel_stg__hotel') }}
),
paises as (
    select * from {{ ref('paises_iso') }}
),
-- Normalizar países una sola vez para mejorar performance
paises_normalizados as (
    select
        trim(upper(adjetivo)) as adjetivo_norm,
        trim(upper(iso_alfa_2)) as iso2_norm,
        trim(upper(nombre_pais)) as nombre_norm,
        trim(upper(iso_alfa_3)) as iso3_norm,
        nombre_pais,
        iso_alfa_3
    from paises
),
hoteles_con_pais as (
    select
        h.*,
        trim(upper(h.pais)) as pais_norm,
        p.nombre_pais,
        p.iso_alfa_3,
        -- ROW_NUMBER para eliminar duplicados si hay múltiples coincidencias
        row_number() over (
            partition by h.id_hotel 
            order by 
                case 
                    when trim(upper(h.pais)) = p.iso2_norm then 1  -- Prioridad a ISO2
                    when trim(upper(h.pais)) = p.iso3_norm then 2  -- Luego ISO3
                    when trim(upper(h.pais)) = p.nombre_norm then 3  -- Luego nombre
                    when trim(upper(h.pais)) = p.adjetivo_norm then 4  -- Finalmente adjetivo
                    else 5
                end
        ) as rn
    from stg_hoteles h
    left join paises_normalizados p 
        on h.pais is not null
        and trim(upper(h.pais)) in (p.iso2_norm, p.iso3_norm, p.nombre_norm, p.adjetivo_norm)
),
final as (
    select
        id_hotel,
        nombre as nombre_hotel,
        direccion,
        ciudad,
        coalesce(nombre_pais, pais) as pais_nombre,
        coalesce(iso_alfa_3, 'N/A') as pais_codigo_iso3,
        telefono,
        email,
        fecha_creacion,
        activo as es_activo,
        categoria as estrellas,
        case 
            when categoria >= 4 then 'Lujo'
            when categoria = 3 then 'Estandar'
            else 'Economico'
        end as segmento_hotel,
        datediff('year', fecha_creacion, current_date()) as anios_operativo,
        current_timestamp() as _dbt_updated_at
    from hoteles_con_pais
    where rn = 1  -- Solo tomamos la primera coincidencia
)
select * from final