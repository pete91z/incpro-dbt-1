{{ config(materialized='incremental',unique_key='sk_id',
   post_hook = ["delete from {{source('stg_ops','stg_ops')}}" ]
)
}}
with dim_ops_final as (
    select * from {{ref('stg_dim_ops_030')}}
)

select * from dim_ops_final