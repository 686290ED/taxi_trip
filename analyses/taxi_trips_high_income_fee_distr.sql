with
    taxis as (
        select taxi_id
        from {{ ref("src_taxi_trips_all") }}
        where
            extract(year from trip_start_timestamp) = 2021
            and extract(month from trip_start_timestamp) = 1
    ),

    top_taxis as (
        select taxi_id, sum(trip_total) as total
        from {{ ref("src_taxi_trips_all") }}
        where
            extract(year from trip_start_timestamp) >= 2021
            and taxi_id in (select taxi_id from taxis)
        group by taxi_id
        order by sum(trip_total) desc
        limit 10
    ),

    fee_distr as (
        select
            taxi_id,
            {%- for percentile in [0, 0.1, 0.25, 0.5, 0.75, 0.9, 1] %}
            percentile_cont(trip_total, {{ percentile }}) over () as
            {%- if loop.first %} min,
            {%- elif loop.last %} max
            {%- else %} percentile{{ (percentile * 100) | int }},
            {% endif -%}
            {% endfor %}
        from {{ ref("src_taxi_trips_all") }}
        where taxi_id in (select taxi_id from top_taxis)

    )

select *
from top_taxis
