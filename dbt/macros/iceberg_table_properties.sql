{% macro iceberg_table_location(layer, table_name) -%}
    {%- set roots = {
        'bronze': var('s3_bronze'),
        'silver': var('s3_silver'),
        'gold': var('s3_gold'),
    } -%}
    {%- set root = roots.get(layer) -%}
    {%- if root is none -%}
        {%- do exceptions.raise_compiler_error("Unsupported medallion layer for Iceberg location: " ~ layer) -%}
    {%- endif -%}
    {{ return("'" ~ root ~ "/warehouse/" ~ layer ~ ".db/" ~ table_name ~ "'") }}
{%- endmacro %}

{% macro iceberg_table_properties(layer, table_name) -%}
    {{ return({
        'location': iceberg_table_location(layer, table_name)
    }) }}
{%- endmacro %}
