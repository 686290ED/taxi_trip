{{ config(materialized="table") }}

with
    loc_combined_station as (
        select
            trip.location as trip_location,
            trip.latitude as trip_latitude,
            trip.longitude as trip_longitude,
            name,
            id,
            state,
            station.latitude as station_latitude,
            station.longitude as station_longitude
        from {{ ref("stg_taxi_trips_coordinates") }} trip
        cross join {{ ref("stg_stations_in_range") }} station
    ),

    loc_combined_station_filter as (
        select *
        from loc_combined_station
        where
            station_latitude between trip_latitude - 0.7 and trip_latitude + 0.7
            and station_longitude between trip_longitude - 0.7 and trip_longitude + 0.7
    ),

    station_distance as (
        select
            trip_location,
            trip_latitude,
            trip_longitude,
            name,
            id,
            state,
            station_latitude,
            station_longitude,
            st_distance(
                st_geogpoint(trip_longitude, trip_latitude),
                st_geogpoint(station_longitude, station_latitude)
            ) as distance_in_meters
        from loc_combined_station_filter
    ),

    coor_station_rn as (
        select
            trip_location,
            trip_latitude,
            trip_longitude,
            name,
            id,
            state,
            station_latitude,
            station_longitude,
            distance_in_meters,
            row_number() over (
                partition by trip_location order by distance_in_meters
            ) as rn
        from station_distance
    ),

    coor_station as (select * from coor_station_rn where rn = 1)
select *
from coor_station
