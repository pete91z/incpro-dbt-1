{{
    config(
        post_hook = ["insert into {{source('control','incr_daily')}} (select '{{ this.identifier }}', max(last_update_date) ,null,'T',{{var('run_id')}} from {{this}})"],
        materialized='incremental'
    )
}}

with person as (
select id,
       first_names||' '||last_name as full_name,
       date_of_birth,ni_number,
       case when substring(ni_number,0,2)='TN' then 'TEMPORARY' else 'PERMANENT' end as ni_status,
       decode(marital_status,'W','Widowed','M','Married','S','Single','Se','Separated','D','Divorced','Error') as marital_status,
       decode(gender,'M','Male','F','Female','U','Unspecified','Error') as gender,
       start_date,
       date_part(year,start_date) start_year,
       status,
       status_review_date,
       leave_date,
       job_id,
       last_update_date
from {{ source('hr', 'person') }}
{% if is_incremental() %}
    where last_update_date::timestamp > (select fetch_timestamp::timestamp from {{ source('control', 'incr_control') }} where model_name='{{ this.identifier }}')
{% endif %}
)
select * from person