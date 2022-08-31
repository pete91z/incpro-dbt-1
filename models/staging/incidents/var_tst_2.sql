{% set results = get_last_increment('fetch_timestamp','stg_person') %}

{{config(post_hook = ["select {{results}}"])}}

select {{results}},1