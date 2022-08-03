/* Variable test - execute as dbt run --select stg_inc --vars "cmd: ('stg_person','stg_ops')" */
{{
    config(
        enabled=false
    )
}}

with incr as (
    select * from {{ source('control', 'incr_control') }}
    where model_name in {{var('cmd')}}
)

select * from incr