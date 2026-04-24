{{ config(materialized='table', properties=iceberg_table_properties('gold', 'gold_production_efficiency')) }}

with weekly_summary as (
    select
        cast(date_trunc('week', actual_start) as date) as week_start,
        line_id,
        count(*) as total_orders,
        sum(case when status = 'COMPLETE' then 1 else 0 end) as completed_orders,
        round(avg(yield), 2) as avg_yield_pct,
        round(avg(cost_per_unit), 2) as avg_cost_per_unit
    from {{ ref('silver_production_orders') }}
    group by 1, 2
),
scored as (
    select
        *,
        case
            when avg_yield_pct >= 95 then 'HIGH'
            when avg_yield_pct >= 85 then 'MEDIUM'
            else 'LOW'
        end as efficiency_category,
        lag(avg_yield_pct) over (partition by line_id order by week_start) as prior_week_yield
    from weekly_summary
)
select
    week_start,
    line_id,
    total_orders,
    completed_orders,
    avg_yield_pct,
    avg_cost_per_unit,
    efficiency_category,
    case
        when prior_week_yield is null then 'STABLE'
        when avg_yield_pct > prior_week_yield then 'IMPROVING'
        when avg_yield_pct < prior_week_yield then 'DECLINING'
        else 'STABLE'
    end as trend_vs_prior_week
from scored
