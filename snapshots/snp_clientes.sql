{% snapshot snp_clientes %}

{{
    config(
        target_database=env_var('DBT_DATABASE_GOLD', 'HOTEL_DEV_GOLD_DB'),
        target_schema='SNAPSHOTS',
        unique_key='id_cliente',
        strategy='check',
        check_cols='all',
    )
}}

select
    id_cliente,
    nombre,
    dni_pasaporte,
    nacionalidad,
    direccion,
    email,
    telefono,
    fecha_registro
from {{ ref('silver_hotel_stg__clientes') }}

{% endsnapshot %}