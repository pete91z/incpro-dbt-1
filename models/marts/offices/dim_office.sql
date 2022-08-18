{{ config(materialized='incremental',unique_key='sk_id',
   post_hook = ["delete from {{source('stg_office','stg_offices')}}" ]
)
}}
with dim_office_final as (
    select * from {{ref('stg_dim_office_030')}}
)

select * from dim_office_final