select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select revenue_monthly_total_amount
from `warm-ring-374916`.`dbt_dimkaart`.`dm_monthly_zone_revenue`
where revenue_monthly_total_amount is null



      
    ) dbt_internal_test