{{
    config(
        materialized='table'
    )
}}

with stg_dim_office_1 as (/* de-dupe and remove null sk entries */
    select sk_id,
           location_id as bk_id,
           location_name,
           location_code,
           location_description,
           location_lat as lat,
           location_long as long,
           last_update_date,
           last_update_date as valid_from,
           1 as load_status 
    from {{ ref('stg_dim_office_010')}}
    where sk_id is not null and order_rank=1
),

stg_dim_office_2 as (
    select sk_id,
           bk_id,
           location_name,
           location_code,
           location_description,
           lat,
           long,
           last_update_date,
           valid_from,
           0 as load_status
    from {{ source('mart_office','dim_office')}} a
    where exists (select 'x' from {{source('stg_office','stg_dim_office_010')}} b where a.bk_id=b.location_id)
)

select 
   sk_id,
   bk_id,
   location_name,
   location_code,
   location_description,
   lat,
   long,
   last_update_date,
   valid_from,
   load_status
 from stg_dim_office_1
 union
 select 
  sk_id,
   bk_id,
   location_name,
   location_code,
   location_description,
   lat,
   long,
   last_update_date,
   valid_from,
   load_status
 from stg_dim_office_2