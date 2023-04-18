-- std precipitation of stations across Chicago each day in 2022
with
    prcp_across as (
        select
            trip_location,
            trip_latitude,
            trip_longitude,
            (wx.date) as date,
            wx.value / 10.0 as prcp
        from {{ ref("stg_taxi_trips_coordinates__station") }} as loc
        join
            `bigquery-public-data.ghcn_d.ghcnd_2022` as wx
            on loc.id = wx.id
            and qflag is null
            and element = 'PRCP'
            and wx.date between '2022-01-01' and '2022-12-31'
    ),

    prcp_std as (select date, stddev_samp(prcp) as std from prcp_across group by date)

select
    min(std) as min_p,
    max(std) as max_p,
    avg(std) as avg_p,
    avg(if(std > 10, 1, 0)) as ratio_5,
    avg(if(std > 10, 1, 0)) as ratio_10,
    avg(if(std > 20, 1, 0)) as ratio_20,
from prcp_std
