select taxi_id
from {{ ref("src_taxi_trips_all") }}
where
    extract(year from trip_start_timestamp) = 2021
    and extract(month from trip_start_timestamp) = 1
