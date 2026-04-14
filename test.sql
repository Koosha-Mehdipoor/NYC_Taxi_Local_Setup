select * from {{ source('raw', 'raw_green_tripdata') }}
limit 5