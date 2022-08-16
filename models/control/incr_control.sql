{{ config(materialized="incremental",unique_key='model_name',
          post_hook = ["delete from {{source('stg_dq','incr_daily')}}"] )}}

with daily_deltas as (
    select distinct model_name,
last_value(fetch_timestamp) over (partition by model_name order by run_id,fetch_timestamp rows between unbounded preceding and unbounded following) as fetch_timestamp,
last_value(fetch_id) over (partition by model_name order by run_id,fetch_timestamp rows between unbounded preceding and unbounded following) as fetch_id,
fetch_type 
from {{source('stg_dq','incr_daily')}} 
)

select * from daily_deltas
