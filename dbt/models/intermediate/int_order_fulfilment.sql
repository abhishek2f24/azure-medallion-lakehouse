{{
    config(
        materialized='incremental',
        unique_key='order_id',
        incremental_strategy='merge',
        tags=['intermediate', 'supply_chain']
    )
}}

with orders as (
    select * from {{ ref('stg_orders') }}
    {% if is_incremental() %}
        where _silver_updated_at > (select max(_silver_updated_at) from {{ this }})
    {% endif %}
),

shipments as (
    select
        order_id,
        shipment_id,
        carrier,
        shipped_at,
        estimated_at,
        delivered_at,
        transit_days,
        freight_cost,
        is_on_time,
        is_delayed,
        status                              as shipment_status,
        row_number() over (
            partition by order_id
            order by shipped_at desc nulls last
        )                                   as ship_rank
    from {{ ref('stg_shipments') }}
),

suppliers as (
    select supplier_id, supplier_name, country as supplier_country, tier as supplier_tier
    from {{ ref('stg_suppliers') }}
    where is_current = true
),

latest_shipment as (
    select * from shipments where ship_rank = 1
),

fulfilment as (
    select
        o.order_id,
        o.order_sk,
        o.customer_id,
        o.supplier_id,
        o.product_id,
        o.order_date,
        o.required_date,
        o.quantity,
        o.unit_price,
        o.total_value,
        o.currency,
        o.order_status,
        o.order_tier,
        o.is_urgent,
        o.region,
        o.source_system,

        -- supplier context
        s.supplier_name,
        s.supplier_country,
        s.supplier_tier,

        -- shipment context
        ls.shipment_id,
        ls.carrier,
        ls.shipped_at,
        ls.estimated_at,
        ls.delivered_at,
        ls.transit_days,
        ls.freight_cost,
        ls.is_on_time,
        ls.is_delayed,
        ls.shipment_status,

        -- fulfilment metrics
        datediff(coalesce(ls.delivered_at::date, current_date()), o.order_date)
                                                            as order_cycle_days,
        datediff(o.required_date, o.order_date)            as order_lead_time_days,
        case
            when ls.delivered_at is null then 'pending'
            when ls.is_on_time            then 'on_time'
            else                               'delayed'
        end                                                 as delivery_outcome,
        case
            when ls.delivered_at is not null and ls.delivered_at::date <= o.required_date
            then true else false
        end                                                 as met_sla,

        -- cost metrics
        coalesce(ls.freight_cost, 0)                        as freight_cost,
        round(coalesce(ls.freight_cost, 0) / nullif(o.total_value, 0) * 100, 2)
                                                            as freight_cost_pct,

        o._ingestion_date,
        current_timestamp()                                 as dbt_updated_at
    from orders o
    left join latest_shipment ls using (order_id)
    left join suppliers s on o.supplier_id = s.supplier_id
)

select * from fulfilment
