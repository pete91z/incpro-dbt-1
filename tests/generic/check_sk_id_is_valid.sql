{% test less_than_zero(model, column_name) %}

with validation as (
     select {{ column_name }} as sk_field 
     from {{model}} 
),
validation_errors as (
     select sk_field
     from validation
     where sk_field<0
)

select * from validation_errors

{% endtest %}
