{{ config(
    materialized='table',
    schema='dimensiones',
    contract={'enforced': true}
) }}

select
    id_servicio,
    nombre_servicio,
    categoria,
    descripcion,
    precio_unitario,
    es_activo,
    case 
        when precio_unitario > 100 then 'PREMIUM'
        else 'BASICO'
    end as categoria_precio_servicio,
    _dbt_loaded_at as _dbt_updated_at
from {{ ref('silver_hotel_stg__servicio') }}