{{
    config(
        materialized='incremental',
        unique_key='order_id',
        incremental_strategy='merge',
        tags=['staging', 'supply_chain']
    )
}}

with source as (
    select * from {{ source('silver', 'orders') }}
    {% if is_incremental() %}
        where _silver_updated_at > (select max(_silver_updated_at) from {{ this }})
    {% endif %}
)

select
    order_id,
    customer_id,
    supplier_id,
    product_id,
    order_date,
    required_date,
    quantity,
    unit_price,
    total_value,
    currency,
    status                                          as order_status,
    region,
    source_system,
    order_tier,
    is_urgent,
    _ingestion_date,
    _silver_updated_at,
    -- surrogate key for joining
    {{ dbt_utils.generate_surrogate_key(['order_id']) }} as order_sk
from source
where order_id is not null
