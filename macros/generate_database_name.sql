--macros/generate_database_name.sql
--nombre EXACTO obligatorio

{% macro generate_database_name(custom_database_name, node) -%}
    {%- if custom_database_name is not none -%}
        {{ custom_database_name | trim }}
    {%- else -%}
        {{ target.database | trim }}
    {%- endif -%}
{%- endmacro %}


--macros/generate_schema_name.sql
--nombre EXACTO obligatorio

--{% macro generate_schema_name(custom_schema_name, node) -%}
--    {%- set default_schema = target.schema -%}
--    {%- if custom_schema_name is not none -%}
--        {{ custom_schema_name | trim }}
--    {%- else -%}
--        {{ default_schema }}
--    {%- endif -%}
--{%- endmacro %}
