{{ config(materialized='table') }}

with mes_batches as (
    select
        batch_id,
        arbitrary(product_code) as product_code,
        arbitrary(product_name) as product_name,
        arbitrary(line_id) as line_id,
        min(event_ts) as production_start,
        max(event_ts) as production_end,
        sum(case when dq_status = 'FAIL' then 1 else 0 end) as dq_fail_events
    from {{ ref('silver_mes_events') }}
    group by 1
),
production_orders as (
    select
        batch_id,
        arbitrary(product_code) as product_code,
        arbitrary(line_id) as line_id,
        max(planned_qty) as planned_qty,
        max(actual_qty) as actual_qty,
        max(yield) as yield_pct,
        max(total_cost) as total_cost,
        max(cost_per_unit) as cost_per_unit
    from {{ ref('silver_production_orders') }}
    group by 1
),
quality_summary as (
    select
        batch_id,
        count(*) as total_deviations,
        sum(case when severity = 'CRITICAL' then 1 else 0 end) as critical_deviations,
        sum(case when is_open_deviation then 1 else 0 end) as open_deviations
    from {{ ref('silver_quality_events') }}
    group by 1
)
select
    mes_batches.batch_id,
    coalesce(production_orders.product_code, mes_batches.product_code) as product_code,
    coalesce(production_orders.line_id, mes_batches.line_id) as line_id,
    production_orders.planned_qty,
    production_orders.actual_qty,
    production_orders.yield_pct,
    production_orders.total_cost,
    production_orders.cost_per_unit,
    coalesce(quality_summary.total_deviations, 0) as total_deviations,
    coalesce(quality_summary.critical_deviations, 0) as critical_deviations,
    case
        when coalesce(quality_summary.critical_deviations, 0) > 0 or mes_batches.dq_fail_events > 0 then 'REJECTED'
        when coalesce(quality_summary.open_deviations, 0) > 0 then 'UNDER_REVIEW'
        else 'RELEASED'
    end as batch_status,
    mes_batches.production_start,
    mes_batches.production_end,
    round(date_diff('second', mes_batches.production_start, mes_batches.production_end) / 3600.0, 2) as cycle_time_hours
from mes_batches
left join production_orders
    on mes_batches.batch_id = production_orders.batch_id
left join quality_summary
    on mes_batches.batch_id = quality_summary.batch_id
