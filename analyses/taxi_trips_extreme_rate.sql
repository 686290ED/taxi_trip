select approx_quantiles(trip_total / trip_seconds, 100)[offset(99)]
from  {{ source("chicago_taxi_trips", "taxi_trips") }}
where trip_seconds > 0
