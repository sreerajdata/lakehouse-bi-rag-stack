{{ config(materialized='table') }}

with batch_summary as (
    select * from {{ ref('gold_batch_summary') }}
),
resolution_summary as (
    select
        batch_id,
        product_code,
        cast(date_trunc('day', reported_ts) as date) as report_date,
        avg(time_to_resolution_hours) as avg_resolution_time_hours
    from {{ ref('silver_quality_events') }}
    group by 1, 2, 3
)
select
    cast(date_trunc('day', batch_summary.production_start) as date) as report_date,
    batch_summary.product_code,
    count(*) as total_batches,
    sum(case when batch_summary.batch_status = 'RELEASED' then 1 else 0 end) as released_batches,
    sum(case when batch_summary.batch_status = 'REJECTED' then 1 else 0 end) as rejected_batches,
    round(sum(case when batch_summary.batch_status = 'RELEASED' then 1 else 0 end) * 100.0 / nullif(count(*), 0), 2) as right_first_time_pct,
    round(avg(batch_summary.yield_pct), 2) as avg_yield_pct,
    sum(batch_summary.total_deviations) as total_deviations,
    round(sum(batch_summary.critical_deviations) * 100.0 / nullif(sum(batch_summary.total_deviations), 0), 2) as critical_deviation_rate,
    round(avg(resolution_summary.avg_resolution_time_hours), 2) as avg_resolution_time_hours
from batch_summary
left join resolution_summary
    on batch_summary.batch_id = resolution_summary.batch_id
group by 1, 2
