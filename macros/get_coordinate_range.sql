{% macro get_coordinate_range() %}

{% set result = ({'max_latitude': 0, 'min_latitude': 0, 'max_longitude': 0, 'min_longitude': 0}) %}

{% for coord in ['latitude', 'longitude'] %}
    {% for op in ['max', 'min'] %}
        {% set query %}
            select {{ op }}({{ coord }})
            from {{ ref('stg_taxi_trips_coordinates') }}
        {% endset %}
        {% set query_result = run_query(query) %}
        {% if execute %}
        -- {% set ks = [op, coord] %}
        -- {% set k = ks | join("_") %}
        {% set _ = result.update({op ~ "_" ~ coord: query_result.columns[0].values()[0]}) %}
        {% endif %}
    {% endfor %}
{% endfor %}

{{ log(result, info=True) }}

{{ return(result) }}

{% endmacro %}