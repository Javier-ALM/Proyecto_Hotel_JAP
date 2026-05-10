{% snapshot snp_clientes %}

{{
    config(
        target_database="{{ env_var('DBT_DATABASE_GOLD') }}",
        target_schema='snapshots',
        unique_key='id_cliente',
        strategy='check',
        check_cols='all',
    )
}}

SELECT
    id_cliente,
    nombre,
    dni_pasaporte,
    nacionalidad,
    direccion,
    email,
    telefono,
    fecha_registro
FROM {{ ref('silver_hotel_stg__clientes') }}

{% endsnapshot %}
