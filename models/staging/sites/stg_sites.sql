{{
    config(
        materialized='incremental',
        post_hook = ["update {{ source('control', 'incr_control') }} set fetch_timestamp= (select ts from (
select 'a',coalesce(max(last_update_date),getdate()) as ts from {{ this }})) where model_name='{{ this.identifier }}'"]
    )
}}

with sites as (
    select site_loc_id as site_id,
       site_name,
       site_desc as site_classification,
       coalesce(postcode,'UNKNOWN') as postcode,
       coalesce(split_part(postcode,' ',1),'UNKNOWN') as postcode_outcode,
       coalesce(nullif(split_part(postcode,' ',2),''),'UNKNOWN') as postcode_incode,
       coalesce(split_part(postcode,' ',1)||' '||substring(split_part(postcode,' ',2),1,1),'UNKNOWN') as sector,
       coalesce(nullif(regexp_substr(split_part(postcode,' ',2),'[A-Z]{2}'),''),'UNKNOWN') as unit,
       coalesce(substring(postcode,1,2),'UNKNOWN') as postcode_area, 
       lat,
       long,
       date_added,
       last_update_date
from {{ source('sites', 'sites') }}
{% if is_incremental() %}
    where last_update_date::timestamp > (select fetch_timestamp::timestamp from {{ source('control', 'incr_control') }} where model_name='{{ this.identifier }}')
{% endif %}
)

select * from sites