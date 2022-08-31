{% macro get_last_increment(column_name,mod_name) %}

{% set inc_query %}
select {{column_name}} from {{ source('control','incr_control') }}
    where model_name='{{mod_name}}'
{% endset %}

{% set results = run_query(inc_query) %}

{% if execute %}
{# Return the first column #}
{% set results_list = results.columns[0].values() %}
{% else %}
{% set results_list = [] %}
{% endif %}

{{ return(results_list[0]) }}

{% do log(results_list[0], info=True) %}

{% endmacro %}
