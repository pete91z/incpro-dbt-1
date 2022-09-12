{{ config(enabled=False)}}

{% set results = get_last_increment('fetch_timestamp','stg_person') %}

select cast('{{results}}' as timestamp) as ts,1 as val