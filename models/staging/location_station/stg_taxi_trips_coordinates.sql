{{ config(materialized="table") }}

with
    pickup_coord as (
        select distinct
            pickup_location as location,
            pickup_latitude as latitude,
            pickup_longitude as longitude
        from `bigquery-public-data.chicago_taxi_trips.taxi_trips`
    ),

    dropoff_coord as (
        select distinct
            dropoff_location as location,
            dropoff_latitude as latitude,
            dropoff_longitude as longitude
        from `bigquery-public-data.chicago_taxi_trips.taxi_trips`
    ),

    coord as (
        select *
        from pickup_coord
        union
        distinct
        select *
        from dropoff_coord
    )

select *
from coord
where location is not null
