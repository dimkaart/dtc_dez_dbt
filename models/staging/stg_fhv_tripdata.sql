{{ config(materialized='view') }} -- materialization strategy (table, view, incremental, ephemeral)

with tripdata as 
(
  select *
  from {{ source('staging','rides_fhv') }}  -- macro for FROM clause in schema.yml
  WHERE PULocationID NOT IN ('nan')
  AND DOLocationID NOT IN ('nan')
)
select
 -- identifiers
    dispatching_base_num,
    safe_cast(pulocationid as numeric) as pickup_locationid,
    safe_cast(dolocationid as numeric) as dropoff_locationid,
  

    -- timestamps
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropoff_datetime as timestamp) as dropoff_datetime,
    
    -- trip info
    cast(IF(SR_Flag in ('nan'), 0, 1) as numeric) as SR_Flag,
    Affiliated_base_number
    

FROM tripdata
WHERE cast(pickup_datetime as timestamp) between timestamp('2019-01-01') and timestamp('2019-12-31')
-- dbt build -m <model.sql> --var 'is_test_run: false'
{% if var('is_test_run', default=false) %} -- variable to run model with or without limiter 
--> dbt run --select <model> --var 'is_test_run: true'

    LIMIT 100

{% endif %}