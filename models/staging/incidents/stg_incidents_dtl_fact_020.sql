{{
    config(
        materialized='table'
    )
}}

with inc_joined_010 as (
    select idf.log_id,
           idf.ios_incident_id,
           idf.operator_id,
           coalesce(op.sk_id,'-1') as operator_sk_id,
           idf.started,
           idf.stopped, 
           idf.period_worked,
           idf.incident_raise_date,
           idf.schedule_update_date,
           idf.notes,
           idf.site_id,
           coalesce(si.sk_id,'-1') as site_sk_id,
           coalesce(ofi.sk_id,'-1') as office_sk_id,
           idf.incident_description,
           idf.inc_keyword_1,
           idf.inc_keyword_2,
           idf.inc_keyword_3,
           idf.risklevel,
           idf.incident_type,
           idf.sched_status as status,
           idf.assigned_date,
           si.valid_from as si_valid_from,
           si.valid_to as si_valid_to
    from {{ref('stg_incidents_dtl_fact_010')}}  idf
    left join {{ref('dim_operators')}} op on (idf.operator_id=op.bk_id)
    left join {{ref('dim_sites')}} si on (idf.site_id=si.bk_id)
    left join {{ref('dim_office')}} ofi on (op.base=ofi.location_code)
    where idf.ios_incident_id is not null and idf.ii_incident_id is not null 
    and idf.schedule_update_date >= op.valid_from and idf.schedule_update_date < op.valid_to
    and idf.schedule_update_date >= si.valid_from and idf.schedule_update_date< si.valid_to
    and idf.schedule_update_date >= ofi.valid_from and idf.schedule_update_date < ofi.valid_to 
)

select * from inc_joined_010