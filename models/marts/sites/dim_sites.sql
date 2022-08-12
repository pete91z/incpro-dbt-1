{{ config(materialized='incremental',unique_key='sk_id',
   post_hook = ["delete from {{source('stg_sites','stg_sites')}}","delete from {{source('stg_sites','stg_sites_extended_attributes')}}" ]
)
}}
with dim_sites_final as (
    select * from {{ref('stg_dim_sites_030')}}
)

select * from dim_sites_final