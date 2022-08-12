{{
    config(
        materialized='table'
    )
}}

with stg_dim_sites_1 as (/* de-dupe and remove null sk entries */
    select sk_id,
           site_id as bk_id,
           site_name,
           site_classification,
           postcode,
           postcode_outcode,
           postcode_incode,
           sector,
           unit,
           postcode_area,
           lat,
           long,
           site_date_added,
           site_status,
           standard_parking,
           blue_badge_parking,
           site_capacity,
           wc,
           accessible_wc,
           shop,
           food_and_drink,
           site_under_maintenance,
           last_update_date,
           last_update_date as valid_from,
           1 as load_status 
    from {{ ref('stg_dim_sites_010')}}
    where sk_id is not null and order_rank=1
),

stg_dim_sites_2 as (
    select sk_id,
           bk_id,
           site_name,
           site_classification,
           postcode,
           postcode_outcode,
           postcode_incode,
           sector,
           unit,
           postcode_area,
           lat,
           long,
           site_date_added,
           site_status,
           standard_parking,
           blue_badge_parking,
           site_capacity,
           wc,
           accessible_wc,
           shop,
           food_and_drink,
           site_under_maintenance,
           last_update_date,
           valid_from,
           0 as load_status
    from {{ source('mart_sites','dim_sites')}} a
    where exists (select 'x' from {{source('stg_sites','stg_dim_sites_010')}} b where a.bk_id=b.site_id)
)

select 
   sk_id,
   bk_id,
   site_name,
   site_classification,
   postcode,
   postcode_outcode,
   postcode_incode,
   sector,
   unit,
   postcode_area,
   lat,
   long,
   site_date_added,
   site_status,
   standard_parking,
   blue_badge_parking,
   site_capacity,
   wc,
   accessible_wc,
   shop,
   food_and_drink,
   site_under_maintenance,
   last_update_date,
   valid_from,
   load_status
 from stg_dim_sites_1
 union
 select 
  sk_id,
   bk_id,
   site_name,
   site_classification,
   postcode,
   postcode_outcode,
   postcode_incode,
   sector,
   unit,
   postcode_area,
   lat,
   long,
   site_date_added,
   site_status,
   standard_parking,
   blue_badge_parking,
   site_capacity,
   wc,
   accessible_wc,
   shop,
   food_and_drink,
   site_under_maintenance,
   last_update_date,
   valid_from,
   load_status
 from stg_dim_sites_2