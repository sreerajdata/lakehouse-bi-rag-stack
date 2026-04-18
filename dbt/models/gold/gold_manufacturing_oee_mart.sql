{{ config(materialized='table') }}

with production as (
    select
        machine_id,
        plant,
        shift,
        date(start_time) as production_date,
        _ingest_year as year,
        _ingest_month as month,
        _ingest_day as day,
        count(*) as total_orders,
        sum(planned_qty) as total_planned_qty,
        sum(actual_qty) as total_actual_qty,
        sum(rejected_qty) as total_rejected_qty,
        avg(yield_pct) as avg_yield_pct,
        avg(scrap_pct) as avg_scrap_pct,
        sum(duration_minutes) as total_production_minutes,
        count_if(status = 'COMPLETED') as completed_orders,
        count_if(status = 'ON_HOLD') as on_hold_orders
    from {{ ref('silver_mes_production_orders') }}
    where status in ('COMPLETED', 'IN_PROGRESS', 'ON_HOLD')
    group by 1, 2, 3, 4, 5, 6, 7
),
quality as (
    select
        machine_id,
        date(tested_at) as quality_date,
        count(*) as total_tests,
        count_if(result = 'PASS') as passed_tests,
        count_if(result = 'FAIL') as failed_tests,
        round(cast(count_if(result = 'PASS') as double) / nullif(count(*), 0) * 100, 2) as pass_rate_pct,
        avg(result_value) as avg_result_value
    from {{ ref('silver_iqms_quality_tests') }}
    group by 1, 2
)
select
    p.machine_id,
    p.plant,
    p.shift,
    p.production_date,
    p.year,
    p.month,
    p.day,
    p.total_orders,
    p.total_planned_qty,
    p.total_actual_qty,
    p.total_rejected_qty,
    p.completed_orders,
    p.on_hold_orders,
    coalesce(p.avg_yield_pct, 0) as avg_yield_pct,
    coalesce(p.avg_scrap_pct, 0) as avg_scrap_pct,
    p.total_production_minutes,
    round(cast(p.completed_orders as double) / nullif(p.total_orders, 0), 4) as availability,
    round(coalesce(p.avg_yield_pct, 0) / 100, 4) as performance,
    coalesce(q.pass_rate_pct, 0) / 100 as quality_rate,
    round(
        (cast(p.completed_orders as double) / nullif(p.total_orders, 0))
        * (coalesce(p.avg_yield_pct, 0) / 100)
        * (coalesce(q.pass_rate_pct, 0) / 100),
        4
    ) as oee_score,
    coalesce(q.total_tests, 0) as quality_tests_run,
    coalesce(q.passed_tests, 0) as quality_tests_passed,
    coalesce(q.pass_rate_pct, 0) as quality_pass_rate_pct,
    current_timestamp as _gold_loaded_at,
    'tpl_lakehouse.dbt' as _data_source
from production p
left join quality q
    on p.machine_id = q.machine_id
   and p.production_date = q.quality_date
