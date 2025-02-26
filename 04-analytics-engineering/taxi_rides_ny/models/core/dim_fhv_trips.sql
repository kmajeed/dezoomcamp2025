{{
    config(
        materialized='table'
    )
}}

with fhv_trips as (
    select
        dispatching_base_num,
        pickup_datetime,
        dropoff_datetime,
        pickup_locationid,
        dropoff_locationid,
        sr_flag,
        affiliated_base_number
    from {{ ref('stg_fhv_tripdata') }}
),

dim_zones as (
    select
        locationid,
        borough,
        zone,
        service_zone
    from {{ ref('dim_zones') }}
    where borough != 'Unknown'
)

select
    fhv_trips.dispatching_base_num,
    fhv_trips.pickup_datetime,
    fhv_trips.dropoff_datetime,
    fhv_trips.pickup_locationid,
    pickup_zone.borough as pickup_borough,
    pickup_zone.zone as pickup_zone,
    fhv_trips.dropoff_locationid,
    dropoff_zone.borough as dropoff_borough,
    dropoff_zone.zone as dropoff_zone,
    fhv_trips.sr_flag,
    fhv_trips.affiliated_base_number,
    extract(year from fhv_trips.pickup_datetime) as year,
    extract(month from fhv_trips.pickup_datetime) as month
from fhv_trips
left join dim_zones as pickup_zone
    on fhv_trips.pickup_locationid = pickup_zone.locationid
left join dim_zones as dropoff_zone
    on fhv_trips.dropoff_locationid = dropoff_zone.locationid