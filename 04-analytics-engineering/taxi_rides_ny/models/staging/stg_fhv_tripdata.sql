{{
    config(
        materialized='view'
    )
}}

with source_data as (
    select
        {{ dbt.safe_cast("dispatching_base_num", "STRING") }} as dispatching_base_num,
        cast(pickup_datetime as timestamp) as pickup_datetime,
        cast(dropOff_datetime as timestamp) as dropoff_datetime,
        {{ dbt.safe_cast("PUlocationID", "INTEGER") }} as pickup_locationid,
        {{ dbt.safe_cast("DOlocationID", "INTEGER") }} as dropoff_locationid,
        {{ dbt.safe_cast("SR_Flag", "STRING") }} as sr_flag,
        {{ dbt.safe_cast("Affiliated_base_number", "STRING") }} as affiliated_base_number
    from {{ source('staging', 'ext_fhv_taxi') }}
),

cleaned_data as (
    select
        trim(dispatching_base_num) as dispatching_base_num, -- Trim trailing whitespace
        pickup_datetime,
        dropoff_datetime,
        pickup_locationid,
        dropoff_locationid,
        sr_flag,
        affiliated_base_number
    from source_data
    where dispatching_base_num is not null
)

select *
from cleaned_data

-- dbt build --select <model_name> --vars '{'is_test_run': 'false'}'
{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}