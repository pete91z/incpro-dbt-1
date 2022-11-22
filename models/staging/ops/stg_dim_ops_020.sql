{{
    config(
        materialized='table'
    )
}}

with stg_dim_ops_1 as (/* de-dupe and remove null sk entries */
    select sk_id,
           operator_id as bk_id,
           operator_name,
           coalesce(status_name,'N/A') status_name,
           startdate,
           email,
           phone,
           base,
           role_description,
           department,
           manager_name,
           location_name,
           a.last_update_date,
           a.last_update_date as valid_from,
           1 as load_status 
    from {{ ref('stg_dim_ops_010')}} a
    left join {{source('mart_ops','operator_status')}} b on (a.status=b.ops_status_id)
    where sk_id is not null and order_rank=1
),

stg_dim_ops_2 as (
    select sk_id,
           bk_id,
           operator_name,
           status_name,
           startdate,
           email,
           phone,
           base,
           role_description,
           department,
           manager_name,
           location_name,
           last_update_date,
           valid_from,
           0 as load_status
    from {{ source('mart_ops','dim_operators')}} x
    where exists (select 'x' from {{source('stg_ops','stg_dim_ops_010')}} y where x.bk_id=y.operator_id)
)

select 
   sk_id,
   bk_id,
   operator_name,
   status_name,
   startdate,
   email,
   phone,
   base,
   role_description,
   department,
   manager_name,
   location_name,
   last_update_date,
   valid_from,
   load_status
 from stg_dim_ops_1
 union
 select 
  sk_id,
   bk_id,
   operator_name,
   status_name,
   startdate,
   email,
   phone,
   base,
   role_description,
   department,
   manager_name,
   location_name,
   last_update_date,
   valid_from,
   load_status
 from stg_dim_ops_2