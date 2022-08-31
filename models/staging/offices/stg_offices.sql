{{
    config(
        post_hook = ["insert into {{source('control','incr_daily')}} (select * from (select '{{ this.identifier }}', max(last_update_date) as last_update_date ,cast(null as int),'T',{{var('run_id')}} from {{this}}) where last_update_date is not null)"],
        materialized='incremental'
    )
}}

with stg_ofc as (
    select location_id,
           location_name,
           location_code,
           location_description,
           lat,
           long,
           last_update_date
    from {{source('src_ofc','hq_locations')}}
    {% if is_incremental() %}
    where last_update_date::timestamp > (select fetch_timestamp::timestamp from {{ source('control', 'incr_control') }} where model_name='{{ this.identifier }}')
    {% endif %}

)
select * from stg_ofc

