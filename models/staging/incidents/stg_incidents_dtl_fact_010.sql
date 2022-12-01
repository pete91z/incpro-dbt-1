{{
    config(
        materialized='table',
        post_hook = ["delete from {{source('inc_fact_build','stg_inc_ops_sched_late')}}",
                     "delete from {{source('inc_fact_build','stg_incidents_incoming_late')}}",
                     "insert into {{source('inc_fact_build','stg_inc_ops_sched_late')}} 
                      (select ios_incident_id,incident_raise_date,schedule_update_date,sched_status,operator_id,log_id,started,stopped,period_worked,notes,ios_last_update_date
                       from {{this}} where ii_incident_id is null )",
                     "insert into {{source('inc_fact_build','stg_incidents_incoming_late')}} 
                      (select ii_incident_id,site_id,incident_description,inc_keyword_1,inc_keyword_2,inc_keyword_3,inc_raised_date,lat,long,postcode,risklevel,site_description,site_name,incident_type,status,assigned_date,ii_last_update_date
                       from {{this}} where ios_incident_id is null)"
                    ])
}}

with dtl_join_010 as (
select ios.sched_status,
ios.incident_id as ios_incident_id,
ios.operator_id,
ios.log_id,
ios.started,
ios.stopped,
ios.period_worked,
ios.incident_raise_date,
ios.schedule_update_date,
ios.notes,
ios.last_update_date as ios_last_update_date,
ii.incident_id as ii_incident_id,
ii.inc_raised_date,
ii.lat,
ii.long,
ii.postcode,
ii.site_id,
ii.incident_description,
ii.site_name,
ii.inc_keyword_1,
ii.inc_keyword_2,
ii.inc_keyword_3,
ii.site_description,
ii.risklevel,
ii.incident_type,
ii.status,
ii.assigned_date,
ii.last_update_date as ii_last_update_date
from (SELECT stg_inc_ops_sched.incident_id, stg_inc_ops_sched.incident_raise_date, stg_inc_ops_sched.schedule_update_date, stg_inc_ops_sched.sched_status, stg_inc_ops_sched.operator_id, stg_inc_ops_sched.log_id, stg_inc_ops_sched.started, stg_inc_ops_sched.stopped, stg_inc_ops_sched.period_worked, stg_inc_ops_sched.notes, stg_inc_ops_sched.last_update_date
   FROM {{ref('stg_inc_ops_sched')}}
UNION 
 SELECT stg_inc_ops_sched_late.incident_id, stg_inc_ops_sched_late.incident_raise_date, stg_inc_ops_sched_late.schedule_update_date, stg_inc_ops_sched_late.sched_status, stg_inc_ops_sched_late.operator_id, stg_inc_ops_sched_late.log_id, stg_inc_ops_sched_late.started, stg_inc_ops_sched_late.stopped, stg_inc_ops_sched_late.period_worked, stg_inc_ops_sched_late.notes, stg_inc_ops_sched_late.last_update_date
   FROM {{source('inc_fact_build','stg_inc_ops_sched_late')}}) ios
full outer join (SELECT stg_incidents_incoming.incident_id, stg_incidents_incoming.site_id, stg_incidents_incoming.incident_description, stg_incidents_incoming.inc_keyword_1, stg_incidents_incoming.inc_keyword_2, stg_incidents_incoming.inc_keyword_3, stg_incidents_incoming.inc_raised_date, stg_incidents_incoming.lat, stg_incidents_incoming.long, stg_incidents_incoming.postcode, stg_incidents_incoming.risklevel, stg_incidents_incoming.site_description, stg_incidents_incoming.site_name, stg_incidents_incoming.incident_type, stg_incidents_incoming.status, stg_incidents_incoming.assigned_date, stg_incidents_incoming.last_update_date
   FROM {{ref('stg_incidents_incoming')}}
UNION 
 SELECT stg_incidents_incoming_late.incident_id, stg_incidents_incoming_late.site_id, stg_incidents_incoming_late.incident_description, stg_incidents_incoming_late.inc_keyword_1, stg_incidents_incoming_late.inc_keyword_2, stg_incidents_incoming_late.inc_keyword_3, stg_incidents_incoming_late.inc_raised_date, stg_incidents_incoming_late.lat, stg_incidents_incoming_late.long, stg_incidents_incoming_late.postcode, stg_incidents_incoming_late.risklevel, stg_incidents_incoming_late.site_description, stg_incidents_incoming_late.site_name, stg_incidents_incoming_late.incident_type, stg_incidents_incoming_late.status, stg_incidents_incoming_late.assigned_date, stg_incidents_incoming_late.last_update_date
   FROM {{source('inc_fact_build','stg_incidents_incoming_late')}}) ii
on (ios.incident_id=ii.incident_id)
)

select * from dtl_join_010