{{ config(materialized='view')}}

with tripdata as 
(
  select *,
    row_number() over(partition by dispatching_base_num, pickup_datetime) as rn
  from {{ source('staging','fhv_tripdata') }}
  where --dispatching_base_num is not null and 
  PULocationID is not null and DOLocationID is not null
)

select
    -- identifiers
    {{ dbt_utils.surrogate_key(['dispatching_base_num', 'pickup_datetime']) }} as trip_id,
    dispatching_base_num,
    cast(PULocationID as integer) as  pickup_locationid,
    cast(DOLocationID as integer) as dropoff_locationid,

    -- timestamps
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropoff_datetime as timestamp) as dropoff_datetime,

    SR_Flag

from tripdata
where rn = 1
