with dim_sites_1 as (/* drive from the stg_sites table */
with site1 as (
               select site_id,
               site_name,
               site_classification,
               postcode,
               postcode_outcode,
               postcode_incode,
               sector,unit,
               postcode_area,
               lat,
               long,
               date_added,
               last_update_date as site_last_update_date
               from {{source('stg_sites','stg_sites')}}
              )
select si.*,
       decode(sea.status,'O','Open','C','Closed','Error') as site_status,
       sea.gen_carpark as standard_parking,
       sea.accessible_parking as blue_badge_parking,
       sea.capacity as site_capacity,
       sea.wc,
       sea.accessible_wc,
       sea.shop,
       sea.victuals as food_and_drink,
       sea.maintenance as site_under_maintenance,
       sea.last_update_date as sea_last_update_date,
       least(si.site_last_update_date,sea.last_update_date) as earliest_update_date
from 
site1 si
left join {{source('sites','sites_extended_attributes')}} sea on (si.site_id=sea.property_id)),

dim_sites_2 as ( /* drive from the site_extended_attributes table*/
with sea1 as (
select site_id,
       site_status,
       standard_parking,
       blue_badge_parking,
       site_capacity,
       wc,
       accessible_wc,
       shop,
       food_and_drink,
       site_under_maintenance,
       last_update_date as sea_last_update_date
from {{source('stg_sites','stg_sites_extended_attributes')}})
select sea.*,
       si.site_name,si.site_desc as site_classification,
       coalesce(postcode,'UNKNOWN') as postcode,
       coalesce(split_part(postcode,' ',1),'UNKNOWN') as postcode_outcode,
       coalesce(nullif(split_part(postcode,' ',2),''),'UNKNOWN') as postcode_incode,
       coalesce(split_part(postcode,' ',1)||' '||substring(split_part(postcode,' ',2),1,1),'UNKNOWN') as sector,
       coalesce(nullif(regexp_substr(split_part(postcode,' ',2),'[A-Z]{2}'),''),'UNKNOWN') as unit,
       coalesce(substring(postcode,1,2),'UNKNOWN') as postcode_area, 
       si.lat,
       si.long,
       si.date_added,
       last_update_date as site_last_update_date,
       least(sea_last_update_date,si.last_update_date) as earliest_update_date
from sea1 sea
left join {{source('sites','sites')}} si on (sea.site_id=si.site_loc_id))

select site_id,
       site_name,
       site_classification,
       postcode,
       postcode_outcode,
       postcode_incode,
       sector,
       unit,
       postcode_area,
       lat,
       long,
       date_added as site_date_added,
       site_status,
       standard_parking,
       blue_badge_parking,
       site_capacity,
       wc,
       accessible_wc,
       shop,
       food_and_drink,
       site_under_maintenance,
       site_last_update_date,
       sea_last_update_date,
       earliest_update_date
from dim_sites_1
union
select site_id,
       site_name,
       site_classification,
       postcode,
       postcode_outcode,
       postcode_incode,
       sector,
       unit,
       postcode_area,
       lat,
       long,
       date_added as site_date_added,
       site_status,
       standard_parking,
       blue_badge_parking,
       site_capacity,
       wc,
       accessible_wc,
       shop,
       food_and_drink,
       site_under_maintenance,
       site_last_update_date,
       sea_last_update_date,
       earliest_update_date
from dim_sites_2