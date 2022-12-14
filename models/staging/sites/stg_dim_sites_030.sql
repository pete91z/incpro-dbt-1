{{ config(materialized='table') 
}}

with dim_sites_val_010 as (
select sk_id,
       lag(sk_id) over (partition by bk_id order by valid_from asc) as prev_sk_id,
       lead(sk_id) over (partition by bk_id order by valid_from asc) as next_sk_id,       
       bk_id,
       last_value(site_name) over (partition by bk_id order by valid_from asc rows between unbounded preceding and unbounded following) as site_name,
       last_value(site_classification) over (partition by bk_id order by valid_from asc rows between unbounded preceding and unbounded following) as site_classification,
       last_value(postcode) over (partition by bk_id order by valid_from asc rows between unbounded preceding and unbounded following) as postcode,
       last_value(postcode_outcode) over (partition by bk_id order by valid_from asc rows between unbounded preceding and unbounded following) as postcode_outcode,
       last_value(postcode_incode) over (partition by bk_id order by valid_from asc rows between unbounded preceding and unbounded following) as postcode_incode,
       last_value(sector) over (partition by bk_id order by valid_from asc rows between unbounded preceding and unbounded following) as sector,
       last_value(unit) over (partition by bk_id order by valid_from asc rows between unbounded preceding and unbounded following) as unit,
       last_value(postcode_area) over (partition by bk_id order by valid_from asc rows between unbounded preceding and unbounded following) as postcode_area,
       last_value(lat) over (partition by bk_id order by valid_from asc rows between unbounded preceding and unbounded following) as lat,
       last_value(long) over (partition by bk_id order by valid_from asc rows between unbounded preceding and unbounded following) as long,
       first_value(site_date_added) over (partition by bk_id order by valid_from asc rows between unbounded preceding and unbounded following) as site_date_added,
       site_status,
       lag(site_status) over (partition by bk_id order by valid_from asc) as prev_site_status,
       lead(site_status) over (partition by bk_id order by valid_from asc) as next_site_status,
       last_value(standard_parking) over (partition by bk_id order by valid_from asc rows between unbounded preceding and unbounded following) as standard_parking,
       last_value(blue_badge_parking) over (partition by bk_id order by valid_from asc rows between unbounded preceding and unbounded following) as blue_badge_parking,
       site_capacity,
       lag(site_capacity) over (partition by bk_id order by valid_from asc) as prev_site_capacity,
       lead(site_capacity) over (partition by bk_id order by valid_from asc) as next_site_capacity,
       wc,
       lag(wc) over (partition by bk_id order by valid_from asc) as prev_wc,
       lead(wc) over (partition by bk_id order by valid_from asc) as next_wc,
       accessible_wc,
       lag(accessible_wc) over (partition by bk_id order by valid_from asc) as prev_accessible_wc,
       lead(accessible_wc) over (partition by bk_id order by valid_from asc) as next_accessible_wc,
       shop,
       lag(shop) over (partition by bk_id order by valid_from asc) as prev_shop,
       lead(shop) over (partition by bk_id order by valid_from asc) as next_shop,
       food_and_drink,
       lag(food_and_drink) over (partition by bk_id order by valid_from asc) as prev_food_and_drink,
       lead(food_and_drink) over (partition by bk_id order by valid_from asc) as next_food_and_drink,
       site_under_maintenance,
       lag(site_under_maintenance) over (partition by bk_id order by valid_from asc) as prev_site_under_maintenance,
       lead(site_under_maintenance) over (partition by bk_id order by valid_from asc) as next_site_under_maintenance,
       last_update_date,
       valid_from,
       lag(valid_from) over (partition by bk_id order by valid_from asc) as prev_valid_from,
       lead(valid_from) over (partition by bk_id order by valid_from asc) as next_valid_from,
       load_status
from {{ref('stg_dim_sites_020')}} 
order by bk_id,valid_from),
dim_sites_val_020 as (
select *,
       case when site_status=prev_site_status then 0 else 1 end as val_prev_site_status,
       case when site_status=next_site_status then 0 else 1 end as val_next_site_status,
       case when site_capacity=prev_site_capacity then 0 else 1 end as val_prev_site_capacity,
       case when site_capacity=next_site_capacity then 0 else 1 end as val_next_site_capacity,
       case when wc=prev_wc then 0 else 1 end as val_prev_wc,
       case when wc=next_wc then 0 else 1 end as val_next_wc,
       case when accessible_wc=prev_accessible_wc then 0 else 1 end as val_prev_accessible_wc,
       case when accessible_wc=next_accessible_wc then 0 else 1 end as val_next_accessible_wc,
       case when shop=prev_shop then 0 else 1 end as val_prev_shop,
       case when shop=next_shop then 0 else 1 end as val_next_shop,
       case when food_and_drink=prev_food_and_drink then 0 else 1 end as val_prev_food_and_drink,
       case when food_and_drink=next_food_and_drink then 0 else 1 end as val_next_food_and_drink,
       case when site_under_maintenance=prev_site_under_maintenance then 0 else 1 end as val_prev_site_under_maintenance,
       case when site_under_maintenance=next_site_under_maintenance then 0 else 1 end as val_next_site_under_maintenance                    
from dim_sites_val_010),
dim_sites_val_030 as (
select *,
       row_number() over (partition by bk_id order by last_update_date asc) rn,
       case when val_prev_site_status+val_prev_site_capacity+val_prev_wc+val_prev_accessible_wc+val_prev_shop+val_prev_food_and_drink+val_prev_site_under_maintenance > 0 then 1 else 0 end prev_check,
       case when val_next_site_status+val_next_site_capacity+val_next_wc+val_next_accessible_wc+val_next_shop+val_next_food_and_drink+val_next_site_under_maintenance > 0 then 1 else 0 end next_check
from dim_sites_val_020)
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
       case when rn=1 then cast('2000-01-01 00:00:00' as timestamp) else last_update_date end valid_from,
       cast(coalesce(lead(valid_from) over (partition by bk_id order by valid_from),'3000-12-31 23:59:59') as timestamp) valid_to,
       case when cast(coalesce(lead(valid_from) over (partition by bk_id order by valid_from),'3000-12-31 23:59:59') as timestamp)=cast('3000-12-31 23:59:59' as timestamp) then 1 else 0 end as current_ind
from dim_sites_val_030
where load_status=0 or (load_status=1 and prev_check>0 and next_check>0)