select *
from {{ ref("src_taxi_trips_all") }}
where
    extract(year from trip_start_timestamp) >= 2021
    and taxi_id in (select taxi_id from {{ ref("stg_taxis_2021") }})
