{{
    config(
        materialized='incremental',
        post_hook = ["insert into {{source('control','incr_daily')}} (select * from (select '{{ this.identifier }}', max(last_update_date) as last_update_date ,cast(null as int),'T',{{var('run_id')}} from {{this}}) where last_update_date is not null)"]
    )
}}

with inc_incoming as (
    select id as incident_id,
       site_loc_id as site_id,
       description as incident_description,
       inc_keyword_1,
       inc_keyword_2,
       inc_keyword_3,
       inc_raised_date,
       lat,
       long,
       postcode,
       risklevel,
       site_desc as site_description,
       site_name,
       type as incident_type,
       status,
       assigned_date,
       last_update_date
    from {{source('incidents','incidents_incoming')}}
{% if is_incremental() %}
    where last_update_date::timestamp > (select fetch_timestamp::timestamp from {{ source('control', 'incr_control') }} where model_name='{{ this.identifier }}')
{% endif %}
)

select * from inc_incoming