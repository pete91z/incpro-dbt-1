{{ config(materialized='incremental',unique_key='log_id',
   post_hook = ["delete from {{source('stg_f_inc','stg_incidents_incoming')}}","delete from {{source('stg_f_inc','stg_inc_ops_sched')}}" ]
)
}}

select log_id,
       ios_incident_id as incident_id,
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
       assigned_date
from {{ref('stg_incidents_dtl_fact_020')}}

