{{
    config(
        materialized='incremental',
        post_hook = ["insert into {{source('control','incr_daily')}} (select * from (select '{{ this.identifier }}', max(last_update_date) as last_update_date ,cast(null as int),'T',{{var('run_id')}} from {{this}}) where last_update_date is not null)"]
    )
}}

with ops as (
    select operator_id,first_name||' '||last_name operator_name,position,status,startdate,email,phone,base,last_update_date
from {{ source('ops', 'operators') }}
{% if is_incremental() %}
    where last_update_date::timestamp > (select fetch_timestamp::timestamp from {{ source('control', 'incr_control') }} where model_name='{{ this.identifier }}')
{% endif %}
)
,
op_role as (
select * from (
select a.*,row_number() over (partition by operator_id order by dept_match_score desc) as rn from (
select ops.operator_id,
ops.operator_name,
ops.position,
ops.status,
ops.startdate,
ops.email,
ops.phone,
ops.base,
ops.last_update_date,
ro.role_description,
hq.location_name,
ou.unit_name as dept,
ou.manager_id,
fuzzy_match(ro.role_description,ou.unit_name) as dept_match_score
from ops
join {{source('ops','role')}} ro on (ops.position=ro.role_id)
join {{source('ops','hq_locations')}} hq on (ops.base=hq.location_code)
join {{source('ops','org_unit')}} ou on (hq.location_id=ou.hq_id)
) a ) where rn=1)

select oro.operator_id,
       oro.operator_name,
       oro.position,
       oro.status,
       oro.startdate,
       oro.email,
       oro.phone,
       oro.base,
       oro.last_update_date,
       oro.role_description,  
       pe.first_names||' '||pe.last_name as manager_name,
       oro.dept as department,
       oro.location_name
from op_role oro
left join {{source('ops','person')}} pe on (oro.manager_id=pe.id)