
  
    

    create or replace table `warm-ring-374916`.`dbt_dimkaart`.`dim_zones`
    
    
    OPTIONS()
    as (
      

SELECT 
    locationid,
    borough,
    zone,
    replace(service_zone, 'Boro','Green') as service_zone

FROM `warm-ring-374916`.`dbt_dimkaart`.`taxi_zone_lookup`
    );
  