with
    yr_income as (
        select
            yr,
            taxi_id,
            sum(trip_total) as total,
            row_number() over (partition by yr order by sum(trip_total) desc) as rnk
        from {{ ref("src_taxi_trips_all") }}
        group by yr, taxi_id
        order by yr desc
    )
select *
from yr_income
where rnk <= 5
order by yr desc, rnk
