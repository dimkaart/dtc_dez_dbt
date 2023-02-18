select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select tripid
from `warm-ring-374916`.`dbt_dimkaart`.`stg_green_tripdata`
where tripid is null



      
    ) dbt_internal_test