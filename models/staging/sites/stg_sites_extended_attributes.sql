{{
    config(
        materialized='incremental',
        post_hook = ["update {{ source('control', 'incr_control') }} set fetch_timestamp= (select ts from (
select 'a',coalesce(max(last_update_date),getdate()) as ts from {{ this }})) where model_name='{{ this }}'"]
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
    where last_update_date::timestamp > (select fetch_timestamp::timestamp from {{ source('control', 'incr_control') }} where model_name='{{ this }}')
{% endif %} 
)

select * from sites_extended_attributes