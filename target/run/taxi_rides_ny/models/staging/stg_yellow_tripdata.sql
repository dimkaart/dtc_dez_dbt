

  create or replace view `warm-ring-374916`.`dbt_dimkaart`.`stg_yellow_tripdata`
  OPTIONS()
  as  -- materialization strategy (table, view, incremental, ephemeral)

with tripdata as 
(
  select *,
    row_number() over(partition by vendorid, tpep_pickup_datetime) as rn
  from `warm-ring-374916`.`dbt_dimkaart`.`rides_yellow`  -- macro for FROM clause in schema.yml
  where vendorid is not null 
)
select
 -- identifiers
    to_hex(md5(cast(coalesce(cast(vendorid as 
    string
), '') || '-' || coalesce(cast(tpep_pickup_datetime as 
    string
), '') as 
    string
))) as tripid, -- call function from loaded package
    cast(vendorid as integer) as vendorid,
    cast(ratecodeid as integer) as ratecodeid,
    cast(pulocationid as integer) as  pickup_locationid,
    cast(dolocationid as integer) as dropoff_locationid,
    
    -- timestamps
    cast(tpep_pickup_datetime as timestamp) as pickup_datetime,
    cast(tpep_dropoff_datetime as timestamp) as dropoff_datetime,
    
    -- trip info
    store_and_fwd_flag,
    cast(passenger_count as integer) as passenger_count,
    cast(trip_distance as numeric) as trip_distance,
    -- yellow cabs are always street-hail
    1 as trip_type,
    
    -- payment info
    cast(fare_amount as numeric) as fare_amount,
    cast(extra as numeric) as extra,
    cast(mta_tax as numeric) as mta_tax,
    cast(tip_amount as numeric) as tip_amount,
    cast(tolls_amount as numeric) as tolls_amount,
    0 as ehail_fee,
    cast(improvement_surcharge as numeric) as improvement_surcharge,
    cast(total_amount as numeric) as total_amount,
    cast(payment_type as integer) as payment_type,
    case payment_type
        when 1 then 'Credit card'
        when 2 then 'Cash'
        when 3 then 'No charge'
        when 4 then 'Dispute'
        when 5 then 'Unknown'
        when 6 then 'Voided trip'
    end

 as payment_type_description, -- macro for calculation in get_payment_type_description.sql
    cast(congestion_surcharge as numeric) as congestion_surcharge 

FROM tripdata
WHERE rn=1
-- dbt build -m <model.sql> --var 'is_test_run: false'
;

