-- top 5 common route in the last 2 years
select
    pickup_location,
    dropoff_location,
    count(*) as cnt,
    avg(trip_total) as avg_trip_total
from {{ ref("src_taxi_trips_all") }}
where
    extract(year from trip_start_timestamp) >= 2021
    and pickup_location is not null
    and dropoff_location is not null
group by pickup_location, dropoff_location
order by cnt desc
limit 5
