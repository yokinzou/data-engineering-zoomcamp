{{ config(
    materialized='incremental',
    unique_key='trip_id',
    partition_by={
        "field": "pickup_datetime",
        "data_type": "timestamp",
        "granularity": "month"
    }
)}}

select
    {{ dbt_utils.generate_surrogate_key(
        ['vendor_id', 'pickup_datetime', 'dropoff_location_id']
    ) }} as trip_id,
    *
from {{ ref('stg_yellow_tripdata_2021-01') }}

{% if is_incremental() %}
    where pickup_datetime > (select max(pickup_datetime) from {{ this }})
{% endif %}