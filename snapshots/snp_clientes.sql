{% snapshot snp_clientes %}

{{
    config(
        target_database=env_var('DBT_DATABASE_SILVER', 'HOTEL_DEV_SILVER_DB'),
        target_schema='SNAPSHOTS',
        unique_key='id_cliente',
        strategy='check',
        check_cols='all',
    )
}}

select
    CAST(C1 AS INTEGER) AS id_cliente,
    C2 AS nombre,
    C3 AS dni_pasaporte,
    C4 AS nacionalidad,
    C5 AS direccion,
    C6 AS email,
    C7 AS telefono,

    COALESCE(
        TRY_TO_DATE(TRIM(C8), 'DD-MM-YYYY'),
        TRY_TO_DATE(TRIM(C8), 'YYYY-MM-DD'),
        TRY_TO_DATE(TRIM(C8), 'DD/MM/YYYY'),
        CURRENT_DATE()
    ) AS fecha_registro
    
from {{ source('hotel_raw', 'RAW_CLIENTE') }}
where C1 != 'id_cliente' 

{% endsnapshot %}