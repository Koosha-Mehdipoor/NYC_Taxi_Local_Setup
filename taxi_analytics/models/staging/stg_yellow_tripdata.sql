with source as (
    select * from {{ source('raw', 'raw_yellow_tripdata') }}
),

renamed as (
    select
        -- identifiers (standardized naming for consistency across yellow/green)
        cast(trip_id as varchar) as trip_id,  -- unique identifier for each trip (added for better traceability)
        cast("VendorID" as integer) as vendor_id,
        cast("RatecodeID" as integer) as rate_code_id,
        cast("PULocationID" as integer) as pickup_location_id,
        cast("DOLocationID" as integer) as dropoff_location_id,

        -- timestamps (standardized naming)
        cast(tpep_pickup_datetime as timestamp) as pickup_datetime,  -- tpep = Taxicab Passenger Enhancement Program (yellow taxis)
        cast(tpep_dropoff_datetime as timestamp) as dropoff_datetime,

        -- trip info
        cast(store_and_fwd_flag as varchar) as store_and_fwd_flag,
        cast(passenger_count as integer) as passenger_count,
        cast(trip_distance as numeric) as trip_distance,

        -- payment info
        cast(fare_amount as numeric) as fare_amount,
        cast(extra as numeric) as extra,
        cast(mta_tax as numeric) as mta_tax,
        cast(tip_amount as numeric) as tip_amount,
        cast(tolls_amount as numeric) as tolls_amount,
        cast(improvement_surcharge as numeric) as improvement_surcharge,
        cast(total_amount as numeric) as total_amount,
        cast(payment_type as integer) as payment_type,
        -- traceability fields
        file_name as file_name,  -- capture source file name for traceability
        ingestion_datetime as ingestion_datetime,  -- capture when the record was ingested for traceability
        updated_datetime as updated_datetime  -- capture when the record was last updated for traceability
    from source
    -- Filter out records with null vendor_id (data quality requirement)
    where "VendorID" is not null
)

select * from renamed

-- Sample records for dev environment using deterministic date filter
{% if target.name == 'dev' %}
where pickup_datetime >= '2026-01-01' and pickup_datetime < '2026-02-01'
{% endif %}