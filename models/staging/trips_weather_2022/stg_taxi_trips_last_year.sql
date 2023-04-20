{% set start_time = "2022-01-01 00:00:00+00" %}
{% set end_time = "2023-01-01 00:00:00.00+00" %}

select *
from {{ ref('src_taxi_trips_all') }}
where
    trip_start_timestamp
    between timestamp('{{ start_time }}') and timestamp('{{ end_time }}')
