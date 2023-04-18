-- taxi fee distribution over the last 2 years
select
    {%- for percentile in [0, 0.1, 0.25, 0.5, 0.75, 0.9, 1] %}
    percentile_cont(trip_total, {{ percentile }}) over () as 
    {%- if loop.first %} min,
    {%- elif loop.last %} max
    {%- else %} percentile{{ (percentile * 100) | int }},{% endif -%}
    {% endfor %}
from {{ ref("src_taxi_trips_all") }}
where extract(year from trip_start_timestamp) >= 2021
limit 1
