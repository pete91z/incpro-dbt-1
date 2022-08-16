{{
    config(
        materialized='incremental',
        post_hook = ["insert into {{source('control','incr_daily')}} (select '{{ this.identifier }}', max(last_update_date) ,null,'T',{{var('run_id')}} from {{this}})"]
    )
}}

with ops as (
    select operator_id,first_name,last_name,position,status,startdate,email,phone,base,last_update_date
from {{ source('ops', 'operators') }}
{% if is_incremental() %}
    where last_update_date::timestamp > (select fetch_timestamp::timestamp from {{ source('control', 'incr_control') }} where model_name='{{ this.identifier }}')
{% endif %}
)

select * from ops