select id,ni_number from {{ref('stg_person')}} where ni_number !~ '[a-z|A-Z]{2}[0-9]{6}[a-z|A-Z]{1}'