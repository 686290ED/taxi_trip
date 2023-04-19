select *, extract(year from trip_start_timestamp) as yr
from {{ source('chicago_taxi_trips', 'taxi_trips') }}}