{{config(materialized='incremental',
         schema='dwcontrol'
        )
}}

with dq_dim_sites_010 as (
    select * from {{source('control_dq','stg_dim_sites_010')}}
    where sk_id is null
)

select a.*,load_id(current_timestamp) from dq_dim_sites_010 a