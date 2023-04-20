with
    yr_t as (
        select yr, payment_type, count(*) over (partition by yr) as yr_cnt
        from {{ ref("src_taxi_trips_all") }}
    ),

    payment_cnt as (
        select yr, payment_type, yr_cnt, count(*) as cnt, count(*) / yr_cnt as ratio
        from yr_t
        group by yr, payment_type, yr_cnt
        order by payment_type, yr
    )

select *
from payment_cnt
