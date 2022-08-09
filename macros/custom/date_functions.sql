{% macro load_id(column_name) %}
    cast(extract(epoch from {{column_name}}) as int)
{% endmacro %}