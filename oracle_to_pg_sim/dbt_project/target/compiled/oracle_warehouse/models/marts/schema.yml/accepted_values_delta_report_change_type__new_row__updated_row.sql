
    
    

with all_values as (

    select
        change_type as value_field,
        count(*) as n_records

    from "warehouse"."marts_marts"."delta_report"
    group by change_type

)

select *
from all_values
where value_field not in (
    'new_row','updated_row'
)


