# Home Work W4 Data Analytics

## Question 1
If you run dbt run --select int_trips_unioned, what models will be built?

option 3: int_trips_unioned only


## Question 2

since 6 is not in the accepted values, the test will fail

option 2: dbt will fail the test, returning a non-zero exit code


## Question 3

`select count(*) from "taxi_rides_ny"."prod"."fct_monthly_zone_revenue"`

option 3: 12,184


## Question 4

```
select pickup_zone from "taxi_rides_ny"."prod"."fct_monthly_zone_revenue"
where service_type = 'Green'
and year(revenue_month) = 2020
order by revenue_monthly_total_amount desc
limit 1
```

option 1: East Harlem North


## Question 5

```
SELECT  sum(total_monthly_trips) FROM "taxi_rides_ny"."prod"."fct_monthly_zone_revenue"
where service_type = 'Green'
AND revenue_month  = '2019-10-01'
```

option 3: 384624


## Question 6

```
select count(*) from "taxi_rides_ny"."prod"."stg_fhv_tripdata"
```

option 2: 43,244,693

stg_fhv_tripdata model:

```
with source as (

    SELECT * FROM {{ source('raw', 'fhv_tripdata') }}

)

select 
    CAST(dispatching_base_num as STRING) as dispatching_base_num,
    CAST(pickup_datetime as TIMESTAMP) as pickup_datetime,
    CAST(dropOff_datetime as TIMESTAMP) as drop_off_datetime,
    CAST(PUlocationID as BIGINT) as pickup_location_id,
    CAST(DOlocationID as BIGINT) as drop_off_location_id,
    CAST(SR_Flag as STRING) as shared_ride_flag,
    CAST(Affiliated_base_number as STRING) as affiliated_base_number,
from source
where dispatching_base_num IS NOT NULL
```