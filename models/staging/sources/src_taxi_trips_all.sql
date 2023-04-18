select *, extract(year from trip_start_timestamp) as yr
from `bigquery-public-data.chicago_taxi_trips.taxi_trips`