{{
    config(
        materialized='table'
    )
}}

with dtl_join_010 as (
select ios.sched_status,
ios.incident_id as ios_incident_id,
ios.operator_id,
ios.log_id,
ios.started,
ios.stopped,
ios.period_worked,
ios.schedule_update_date,
ios.last_update_date as ios_last_update_date,
ii.incident_id as ii_incident_id,
ii.inc_raised_date,
ii.site_id,
ii.inc_keyword_1,
ii.inc_keyword_2,
ii.inc_keyword_3,
ii.risklevel,
ii.incident_type,
ii.status,
ii.assigned_date,
ii.last_update_date as ii_last_update_date
from {{source('inc_fact_build','stg_incident_ops_schedule_vw')}} ios
full outer join {{source('inc_fact_build','stg_incidents_incoming_vw')}} ii
on (ios.incident_id=ii.incident_id)
)

select * from dtl_join_010;