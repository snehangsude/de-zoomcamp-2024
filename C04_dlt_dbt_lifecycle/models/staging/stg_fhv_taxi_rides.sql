{{ config(materialized="view") }}

with

    source as (select * from {{ source("staging", "fhv_taxi_rides") }}),

    renamed as (

        select
            dispatching_base_num,
            cast(pickup_datetime as timestamp) as pickup_datetime,
            cast(drop_off_datetime as timestamp) as dropoff_datetime,
            {{ dbt.safe_cast("p_ulocation_id", api.Column.translate_type("integer")) }}
            as pickup_locationid,
            {{ dbt.safe_cast("d_olocation_id", api.Column.translate_type("integer")) }}
            as dropoff_locationid,
            sr_flag,
            affiliated_base_number

        from source

    )

select *
from renamed
where extract(year from pickup_datetime) = 2019

-- dbt build --select <model_name> --vars '{'is_test_run': 'false'}'
{% if var("is_test_run", default=true) %} limit 100 {% endif %}
