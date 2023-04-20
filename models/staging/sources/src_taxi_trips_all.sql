with
    rate as (
        select *, trip_total / trip_seconds as second_rate
        from {{ source("chicago_taxi_trips", "taxi_trips") }}
        where trip_seconds is not null and trip_seconds > 60
    ),

    trips_filtered as (
        select * except (second_rate), extract(year from trip_start_timestamp) as yr
        from rate
        where second_rate <= 1
    )

select *
from trips_filtered
