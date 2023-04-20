with
    top_taxis as (
        select taxi_id, sum(trip_total) as total
        from {{ ref("stg_trips_2021") }}
        group by taxi_id
        order by sum(trip_total) desc
        limit 100
    ),

    top_taxis_trips as (
        select *
        from {{ ref("stg_trips_2021") }}
        where taxi_id in (select taxi_id from top_taxis)
    ),

    fee_distr as (
        select taxi_id, approx_quantiles(trip_total, 4)
        from top_taxis_trips
        group by taxi_id
    ),

    trip_num as (select taxi_id, count(*) as cnt from top_taxis_trips group by taxi_id),

    daily_income as (
        select
            taxi_id,
            extract(date from trip_start_timestamp) as dt,
            sum(trip_total) as daily_total
        from top_taxis_trips
        group by taxi_id, extract(date from trip_start_timestamp)
    ),

    daily_income_distr as (select approx_quantiles(daily_total, 8) from daily_income)

select *
from daily_income_distr

-- select *
-- from top_taxis_trips
-- join
-- (select taxi_id, dt
-- from daily_income
-- where daily_total > 10000) income
-- on extract(date from top_taxis_trips.trip_start_timestamp) = income.dt
-- and top_taxis_trips.taxi_id = income.taxi_id 
