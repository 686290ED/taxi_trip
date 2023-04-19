select approx_quantiles(trip_total / trip_seconds, 100)[offset(99)]
from {{ ref("stg_trips_2021") }}
where trip_seconds > 0
