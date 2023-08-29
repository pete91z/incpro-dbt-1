{{ config(materialized='incremental',unique_key='log_id',dist='inc_raised_sk_id', sort_type='interleaved',sort=['incident_type,','inc_keyword_1'],
   post_hook = ["delete from {{source('stg_f_inc','stg_incidents_incoming')}}","delete from {{source('stg_f_inc','stg_inc_ops_sched')}}" ],
   tags = ["fact","marts"]
)
}}

select log_id,
       ios_incident_id as incident_id,
       cast(to_char(incident_raise_date,'YYYYMMDDHH') as bigint) as inc_raised_sk_id,
       cast(to_char(started,'YYYYMMDDHH') as bigint) as log_started_sk_id,
       cast(to_char(stopped,'YYYYMMDDHH') as bigint) as log_stopped_sk_id,
       cast(to_char(assigned_date,'YYYYMMDDHH') as bigint) as inc_assigned_sk_id,
       operator_sk_id,
       started,
       stopped,
       period_worked,
       incident_raise_date,
       schedule_update_date,
       notes,
       site_sk_id,
       office_sk_id,
       incident_description,
       inc_keyword_1,
       inc_keyword_2,
       inc_keyword_3,
       risklevel,
       incident_type,
       status,
       assigned_date,
       current_timestamp as load_ts
from {{ref('stg_incidents_dtl_fact_020')}}

