{{
    config(
        materialized='table'
    )
}}

with stg_dim_ofc_010 as (
     select sha(location_id||location_name||current_timestamp) as sk_id,
     *
     from {{ref('stg_offices')}}
)
select a.*,
       rank() over (partition by location_id,
                                 location_name,
                                 location_code,
                                 location_description,
                                 location_lat,
                                 location_long
                                 order by last_update_date) as order_rank
from stg_dim_ofc_010  a                               