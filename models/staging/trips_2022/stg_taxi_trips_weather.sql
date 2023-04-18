{{ config(materialized="table") }}

select trip.*, station.name, station.id, wx.value / 10.0 as prcp
from {{ ref("stg_taxi_trips_last_year") }} as trip
left join
    {{ ref("stg_taxi_trips_coordinates__station") }} as station
    on trip.pickup_location = station.trip_location
left join
    `bigquery-public-data.ghcn_d.ghcnd_2022` as wx
    on station.id = wx.id
    and extract(date from trip.trip_start_timestamp) = wx.date
    and wx.qflag is null
    and wx.element = 'PRCP'
