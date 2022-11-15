{{
    config(
        materialized='table'
    )
}}

with stg_dim_ops_010 as (
     select sha(operator_id||operator_name||current_timestamp) as sk_id,
     *
     from {{ref('stg_ops')}}
)
select a.*,
       rank() over (partition by operator_id,
                                 operator_name,
                                 position,
                                 status,
                                 startdate,
                                 email,
                                 phone,
                                 base,
                                 role_description,
                                 department,
                                 manager_name,
                                 location_name
                                 order by last_update_date) as order_rank
from stg_dim_ops_010  a       