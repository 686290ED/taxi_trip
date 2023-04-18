{{ config(materialized="table") }}

{% set coordinate_range = get_coordinate_range() %}

with
    stations_in_range as (
        select *
        from {{ ref("src_ghcn_stations") }}
        where
            latitude
            between {{ coordinate_range["min_latitude"] }}
            - 0.05 and {{ coordinate_range["max_latitude"] }}
            + 0.05
            and longitude
            between {{ coordinate_range["min_longitude"] }} -0.05
            and {{ coordinate_range["max_longitude"] }}
            + 0.05
    )
select *
from stations_in_range
