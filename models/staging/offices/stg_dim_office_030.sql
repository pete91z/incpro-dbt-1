{{ config(materialized='table') 
}}

with dim_office_val_010 as (/* validate current vs previous and next for SCD2 */
select sk_id,
       lag(sk_id) over (partition by bk_id order by valid_from asc) as prev_sk_id,
       lead(sk_id) over (partition by bk_id order by valid_from asc) as next_sk_id,       
       bk_id,
       last_value(location_name) over (partition by bk_id order by valid_from asc rows between unbounded preceding and unbounded following) as location_name,
       last_value(location_code) over (partition by bk_id order by valid_from asc rows between unbounded preceding and unbounded following) as location_code,
       last_value(location_description) over (partition by bk_id order by valid_from asc rows between unbounded preceding and unbounded following) as location_description,
       lat,
       lag(lat) over (partition by bk_id order by valid_from asc) as prev_lat,
       lead(lat) over (partition by bk_id order by valid_from asc) as next_lat,
       long,
       lag(long) over (partition by bk_id order by valid_from asc) as prev_long,
       lead(long) over (partition by bk_id order by valid_from asc) as next_long,
       last_update_date,
       valid_from,
       lag(valid_from) over (partition by bk_id order by valid_from asc) as prev_valid_from,
       lead(valid_from) over (partition by bk_id order by valid_from asc) as next_valid_from,
       load_status
from {{ref('stg_dim_office_020')}} 
order by bk_id,valid_from),
dim_office_val_020 as (
select *,
       case when lat=prev_lat then 0 else 1 end as val_prev_lat,
       case when lat=next_lat then 0 else 1 end as val_next_lat,
       case when long=prev_long then 0 else 1 end as val_prev_long,
       case when long=next_long then 0 else 1 end as val_next_long                    
from dim_office_val_010),
dim_office_val_030 as (
select *,
       row_number() over (partition by bk_id order by last_update_date desc) rn, 
       case when val_prev_lat+val_prev_long > 0 then 1 else 0 end prev_check,
       case when val_next_lat+val_next_long > 0 then 1 else 0 end next_check
from dim_office_val_020)
select sk_id,
       bk_id,
       location_name,
       location_code,
       location_description,
       lat,
       long,
       last_update_date,
       case when rn=1 then cast('1900-01-01 00:00:00' as timestamp) else last_update_date end valid_from,
       cast(coalesce(lead(valid_from) over (partition by bk_id order by valid_from),'3000-12-31 23:59:59') as timestamp) valid_to,
       case when cast(coalesce(lead(valid_from) over (partition by bk_id order by valid_from),'3000-12-31 23:59:59') as timestamp)=cast('3000-12-31 23:59:59' as timestamp) then 1 else 0 end as current_ind
from dim_office_val_030
where load_status=0 or (load_status=1 and prev_check>0 and next_check>0)