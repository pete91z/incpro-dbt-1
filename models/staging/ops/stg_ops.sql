{{
    config(
        materialized='incremental',
        post_hook = ["insert into {{source('control','incr_daily')}} (select * from (select '{{ this.identifier }}', max(last_update_date) as last_update_date ,cast(null as int),'T',{{var('run_id')}} from {{this}}) where last_update_date is not null)"]
    )
}}

with ops as (
    select operator_id,first_name,last_name,position,status,startdate,email,phone,base,last_update_date
from {{ source('ops', 'operators') }}
{% if is_incremental() %}
    where last_update_date::timestamp > (select fetch_timestamp::timestamp from {{ source('control', 'incr_control') }} where model_name='{{ this.identifier }}')
{% endif %}
)

select a.*,b.role_description from ops a
join {{source('ops','role')}} b 
on a.position=b.role_id