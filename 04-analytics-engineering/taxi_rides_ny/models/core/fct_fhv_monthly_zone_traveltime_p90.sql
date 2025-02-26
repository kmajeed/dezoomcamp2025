{{
    config(
        materialized='table'
    )
}}

with trip_durations as (
    select
        year,
        month,
        pickup_locationid,
        dropoff_locationid,
        timestamp_diff(dropoff_datetime, pickup_datetime, SECOND) as trip_duration
    from {{ ref('dim_fhv_trips') }}
    where pickup_locationid is not null and dropoff_locationid is not null --filter out nulls
),

p90_durations as (
    select
        year,
        month,
        pickup_locationid,
        dropoff_locationid,
        PERCENTILE_CONT(trip_duration, 0.90) OVER (PARTITION BY year, month, pickup_locationid, dropoff_locationid) as p90_trip_duration
    from trip_durations
),

zone_names as (
    select
        locationid,
        zone
    from {{ ref('dim_zones') }}
    where borough != 'Unknown'
)

select
    p90_durations.year,
    p90_durations.month,
    p90_durations.pickup_locationid,
    pickup_zone.zone as pickup_zone,
    p90_durations.dropoff_locationid,
    dropoff_zone.zone as dropoff_zone,
    p90_durations.p90_trip_duration
from p90_durations
left join zone_names as pickup_zone
    on p90_durations.pickup_locationid = pickup_zone.locationid
left join zone_names as dropoff_zone
    on p90_durations.dropoff_locationid = dropoff_zone.locationid
order by year, month, pickup_locationid, dropoff_locationid