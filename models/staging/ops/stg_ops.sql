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

select op.*,
       ro.role_description,  
       pe.first_names||' '||pe.last_name as manager_name,
       ou.unit_name as department,
       hq.location_name
from ops op
left join {{source('ops','role')}} ro on (op.position=ro.role_id)
left join {{source('ops','hq_locations')}} hq on (op.base=hq.location_code)
left join {{source('ops','org_unit')}} ou on (lower(substring(ro.role_description,1,4))=lower(substring(ou.unit_name,1,4))) and (hq.location_id=ou.hq_id)
left join {{source('ops','person')}} pe on (ou.manager_id=pe.id)