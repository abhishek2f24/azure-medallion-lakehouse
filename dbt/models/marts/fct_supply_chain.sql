{{
    config(
        materialized='incremental',
        unique_key='order_id',
        incremental_strategy='merge',
        tags=['marts', 'finance', 'operations'],
        post_hook="alter table {{ this }} cluster by (order_date, region)"
    )
}}

with fulfilment as (
    select * from {{ ref('int_order_fulfilment') }}
    {% if is_incremental() %}
        where dbt_updated_at > (select max(dbt_updated_at) from {{ this }})
    {% endif %}
),

supplier_perf as (
    select
        supplier_id,
        count(order_id)                                                     as total_orders,
        round(avg(case when met_sla then 1.0 else 0.0 end) * 100, 2)       as sla_rate_pct,
        round(avg(order_cycle_days), 1)                                     as avg_cycle_days,
        round(avg(freight_cost_pct), 2)                                     as avg_freight_cost_pct,
        sum(total_value)                                                    as total_gmv
    from fulfilment
    where order_status not in ('cancelled')
    group by 1
),

dim_date as (
    select
        order_date,
        dayofweek(order_date)                                               as day_of_week,
        weekofyear(order_date)                                              as week_of_year,
        month(order_date)                                                   as month_number,
        quarter(order_date)                                                 as quarter_number,
        year(order_date)                                                    as year_number,
        case when dayofweek(order_date) in (1, 7) then true else false end  as is_weekend
    from fulfilment
    group by 1
),

final as (
    select
        f.order_id,
        f.order_sk,
        f.customer_id,
        f.supplier_id,
        f.product_id,
        f.order_date,
        f.required_date,
        f.quantity,
        f.unit_price,
        f.total_value,
        f.currency,
        f.order_status,
        f.order_tier,
        f.is_urgent,
        f.region,
        f.source_system,
        f.supplier_name,
        f.supplier_country,
        f.supplier_tier,
        f.shipment_id,
        f.carrier,
        f.shipped_at,
        f.estimated_at,
        f.delivered_at,
        f.transit_days,
        f.freight_cost,
        f.is_on_time,
        f.is_delayed,
        f.shipment_status,
        f.order_cycle_days,
        f.order_lead_time_days,
        f.delivery_outcome,
        f.met_sla,
        f.freight_cost_pct,

        -- supplier performance context
        sp.sla_rate_pct                                         as supplier_sla_rate_pct,
        sp.avg_cycle_days                                       as supplier_avg_cycle_days,
        sp.total_orders                                         as supplier_total_orders,
        case
            when sp.sla_rate_pct >= 95 then 'A'
            when sp.sla_rate_pct >= 85 then 'B'
            when sp.sla_rate_pct >= 70 then 'C'
            else                            'D'
        end                                                     as supplier_grade,

        -- date dimensions
        d.day_of_week,
        d.week_of_year,
        d.month_number,
        d.quarter_number,
        d.year_number,
        d.is_weekend,

        f._ingestion_date,
        f.dbt_updated_at
    from fulfilment f
    left join supplier_perf sp using (supplier_id)
    left join dim_date d using (order_date)
)

select * from final
