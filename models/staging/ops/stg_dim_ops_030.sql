{{ config(materialized='table') 
}}

with dim_operators_val_010 as (
select sk_id,
       lag(sk_id) over (partition by bk_id order by valid_from asc) as prev_sk_id,
       lead(sk_id) over (partition by bk_id order by valid_from asc) as next_sk_id,       
       bk_id,
       last_value(operator_name) over (partition by bk_id order by valid_from asc rows between unbounded preceding and unbounded following) as operator_name,
       last_value(status_name) over (partition by bk_id order by valid_from asc rows between unbounded preceding and unbounded following) as status_name,
       last_value(startdate) over (partition by bk_id order by valid_from asc rows between unbounded preceding and unbounded following) as startdate,
       last_value(email) over (partition by bk_id order by valid_from asc rows between unbounded preceding and unbounded following) as email,
       last_value(phone) over (partition by bk_id order by valid_from asc rows between unbounded preceding and unbounded following) as phone,
       base,
       lag(base) over (partition by bk_id order by valid_from asc) as prev_base,
       lead(base) over (partition by bk_id order by valid_from asc) as next_base,
       role_description,
       lag(role_description) over (partition by bk_id order by valid_from asc) as prev_role_description,
       lead(role_description) over (partition by bk_id order by valid_from asc) as next_role_description,
       department,
       lag(department) over (partition by bk_id order by valid_from asc) as prev_department,
       lead(department) over (partition by bk_id order by valid_from asc) as next_department,
       manager_name,
       lag(manager_name) over (partition by bk_id order by valid_from asc) as prev_manager_name,
       lead(manager_name) over (partition by bk_id order by valid_from asc) as next_manager_name,
       location_name,
       lag(location_name) over (partition by bk_id order by valid_from asc) as prev_location_name,
       lead(location_name) over (partition by bk_id order by valid_from asc) as next_location_name,
       last_update_date,
       valid_from,
       lag(valid_from) over (partition by bk_id order by valid_from asc) as prev_valid_from,
       lead(valid_from) over (partition by bk_id order by valid_from asc) as next_valid_from,
       load_status
from {{ref('stg_dim_ops_020')}} 
order by bk_id,valid_from),

dim_operators_val_020 as (
select *,
       case when base=prev_base then 0 else 1 end as val_prev_base,
       case when base=next_base then 0 else 1 end as val_next_base,
       case when role_description=prev_role_description then 0 else 1 end as val_prev_role_description,
       case when role_description=next_role_description then 0 else 1 end as val_next_role_description,
       case when department=prev_department then 0 else 1 end as val_prev_department,
       case when department=next_department then 0 else 1 end as val_next_department,
       case when manager_name=prev_manager_name then 0 else 1 end as val_prev_manager_name,
       case when manager_name=next_manager_name then 0 else 1 end as val_next_manager_name,
       case when location_name=prev_location_name then 0 else 1 end as val_prev_location_name,
       case when location_name=next_location_name then 0 else 1 end as val_next_location_name 
from dim_operators_val_010),

dim_operators_val_030 as (
select *,
       case when val_prev_base+val_prev_role_description+val_prev_department+val_prev_manager_name+val_prev_location_name > 0 then 1 else 0 end prev_check,
       case when val_next_base+val_next_role_description+val_next_department+val_next_manager_name+val_next_location_name > 0 then 1 else 0 end next_check
from dim_operators_val_020)    
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
       cast(coalesce(lead(valid_from) over (partition by bk_id order by valid_from),'3000-12-31 23:59:59') as timestamp) valid_to,
       case when cast(coalesce(lead(valid_from) over (partition by bk_id order by valid_from),'3000-12-31 23:59:59') as timestamp)=cast('3000-12-31 23:59:59' as timestamp) then 1 else 0 end as current_ind
from dim_operators_val_030
where load_status=0 or (load_status=1 and prev_check>0 and next_check>0)