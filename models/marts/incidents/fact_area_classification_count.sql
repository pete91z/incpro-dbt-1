{{ config(materialized='incremental',unique_key='raise_date',dist='raise_date',sort='raise_date') }}
{% set areas = ["assets", "structural", "facilities","utilities","environment"] %}

with area_count as (
select * from (select cast(incident_raise_date as date) as raise_date
                      {% for area in areas %}
                      ,sum(case when inc_keyword_1 = '{{area}}' then 1 else 0 end) as {{area}}
                      {% endfor %}
from {{ref('fact_incidents_dtl')}}

group by cast(incident_raise_date as date))
)

select * from area_count order by raise_date
