{{
    config(
        materialized='incremental',
        post_hook = ["update {{ source('control', 'incr_control') }} set fetch_timestamp= (select ts from (
select 'a',coalesce(max(last_update_date),getdate()) as ts from {{ this }})) where model_name='{{ this.identifier }}'"]
    )
}}

with incopssched as (
select incident_id,
incident_raise_date,
schedule_update_date,
sched_status,
operator_id,
log_id,
started,
coalesce(stopped,current_timestamp) as stopped,
cast(cast(coalesce(stopped,current_timestamp)-started as varchar) as bigint)/1000000 as period_worked,
notes,
last_update_date
from {{ source('incidents', 'inc_ops_schedule') }}
{% if is_incremental() %}
    where last_update_date::timestamp > (select fetch_timestamp::timestamp from {{ source('control', 'incr_control') }} where model_name='{{ this.identifier }}')
{% endif %}
)

select * from incopssched