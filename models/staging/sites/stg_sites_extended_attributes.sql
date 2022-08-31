
{{
    config(
        materialized='incremental',
        post_hook = ["insert into {{source('control','incr_daily')}} (select * from (select '{{ this.identifier }}', max(last_update_date) as last_update_date ,cast(null as int),'T',{{var('run_id')}} from {{this}}) where last_update_date is not null)"]
    )
}}

with sites_extended_attributes as (
select property_id as site_id,
       decode(status,'O','Open','C','Closed','Error') as site_status,
       gen_carpark as standard_parking,
       accessible_parking as blue_badge_parking,
       capacity as site_capacity,
       wc,
       accessible_wc,
       shop,
       victuals as food_and_drink,
       maintenance as site_under_maintenance,
       last_update_date 
from {{ source('sites', 'sites_extended_attributes') }}
{% if is_incremental() %}
    where last_update_date::timestamp > (select fetch_timestamp::timestamp from {{ source('control', 'incr_control') }} where model_name='{{ this.identifier }}')
{% endif %} 
)

select * from sites_extended_attributes