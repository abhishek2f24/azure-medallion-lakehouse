{{
    config(
        materialized='table',
        tags=['marts', 'supplier', 'reporting']
    )
}}

with base as (
    select
        supplier_id,
        supplier_name,
        supplier_country,
        supplier_tier,
        count(order_id)                                                         as total_orders,
        count(order_id) filter (where order_status = 'delivered')               as delivered_orders,
        count(order_id) filter (where order_status = 'cancelled')               as cancelled_orders,
        sum(total_value)                                                         as total_gmv,
        sum(total_value) filter (where order_status = 'delivered')              as delivered_gmv,
        round(avg(case when met_sla then 1.0 else 0.0 end) * 100, 2)           as on_time_delivery_pct,
        round(avg(order_cycle_days), 1)                                          as avg_cycle_days,
        min(order_cycle_days)                                                    as min_cycle_days,
        max(order_cycle_days)                                                    as max_cycle_days,
        round(avg(freight_cost), 2)                                              as avg_freight_cost,
        round(avg(freight_cost_pct), 2)                                          as avg_freight_cost_pct,
        round(
            count(order_id) filter (where order_status = 'cancelled')::double
            / nullif(count(order_id), 0) * 100, 2
        )                                                                        as cancellation_rate_pct,
        -- rolling 90-day SLA (more recent = more weight)
        round(avg(case when met_sla then 1.0 else 0.0 end) filter (
            where order_date >= current_date() - interval '90 days'
        ) * 100, 2)                                                              as sla_rate_90d_pct
    from {{ ref('fct_supply_chain') }}
    group by 1, 2, 3, 4
),

scored as (
    select
        *,
        case
            when on_time_delivery_pct >= 95 then 5
            when on_time_delivery_pct >= 88 then 4
            when on_time_delivery_pct >= 78 then 3
            when on_time_delivery_pct >= 65 then 2
            else 1
        end                                                                      as delivery_score,
        case
            when avg_freight_cost_pct <= 3  then 5
            when avg_freight_cost_pct <= 5  then 4
            when avg_freight_cost_pct <= 8  then 3
            when avg_freight_cost_pct <= 12 then 2
            else 1
        end                                                                      as cost_score,
        case
            when avg_cycle_days <= 3  then 5
            when avg_cycle_days <= 7  then 4
            when avg_cycle_days <= 14 then 3
            when avg_cycle_days <= 21 then 2
            else 1
        end                                                                      as speed_score
    from base
),

final as (
    select
        *,
        round((delivery_score * 0.5 + cost_score * 0.3 + speed_score * 0.2), 2) as composite_score,
        case
            when (delivery_score * 0.5 + cost_score * 0.3 + speed_score * 0.2) >= 4.5 then 'Preferred'
            when (delivery_score * 0.5 + cost_score * 0.3 + speed_score * 0.2) >= 3.5 then 'Approved'
            when (delivery_score * 0.5 + cost_score * 0.3 + speed_score * 0.2) >= 2.5 then 'Conditional'
            else 'At Risk'
        end                                                                       as supplier_status,
        dense_rank() over (order by
            (delivery_score * 0.5 + cost_score * 0.3 + speed_score * 0.2) desc
        )                                                                         as global_rank
    from scored
)

select * from final
order by composite_score desc
