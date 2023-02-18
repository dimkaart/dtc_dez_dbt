

SELECT 
    locationid,
    borough,
    zone,
    replace(service_zone, 'Boro','Green') as service_zone

FROM `warm-ring-374916`.`dbt_dimkaart`.`taxi_zone_lookup`