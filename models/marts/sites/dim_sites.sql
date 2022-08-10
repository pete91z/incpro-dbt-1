{{ config(materialized='incremental',unique_key='sk_id')
}}
with dim_sites_final as (
    select * from {{source('stg_sites','stg_dim_sites_030')}}
)

select * from dim_sites_final