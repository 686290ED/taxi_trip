with
    -- taxi fee distribution over the last 2 years
    fee_distr as (
        select
            {%- for percentile in [0, 0.1, 0.25, 0.5, 0.75, 0.9, 1] %}
            percentile_cont(trip_total, {{ percentile }}) over (
            ) as {%- if loop.first %} min,
            {%- elif loop.last %} max
            {%- else %} percentile{{ (percentile * 100) | int }},
            {% endif -%}
            {% endfor %}
        from {{ ref("src_taxi_trips_all") }}
        where extract(year from trip_start_timestamp) >= 2021
        limit 1
    ),

    trip_num as (
        select taxi_id, count(*) as cnt
        from {{ ref("stg_trips_2021") }}
        group by taxi_id
    ),

    trip_num_distr as (select approx_quantiles(cnt, 8) from trip_num),

    daily_income as (
        select
            taxi_id,
            extract(date from trip_start_timestamp),
            sum(trip_total) as daily_total
        from {{ ref("stg_trips_2021") }}
        group by taxi_id, extract(date from trip_start_timestamp)
    ),

    daily_income_distr as (select approx_quantiles(daily_total, 8) from daily_income)
-- [0.0, 92.4, 142.25, 186.1, 227.78, 273.85, 329.34000000000003, 413.42,
-- 110210.41999999998]
select *
from daily_income_distr
