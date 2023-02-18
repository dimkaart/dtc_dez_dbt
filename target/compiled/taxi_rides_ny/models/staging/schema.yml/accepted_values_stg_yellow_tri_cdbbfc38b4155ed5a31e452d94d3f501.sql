
    
    

with all_values as (

    select
        Payment_type as value_field,
        count(*) as n_records

    from `warm-ring-374916`.`dbt_dimkaart`.`stg_yellow_tripdata`
    group by Payment_type

)

select *
from all_values
where value_field not in (
    1,2,3,4,5,6
)


