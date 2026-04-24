{{ config(materialized='table', properties=iceberg_table_properties('gold', 'gold_quality_risk_mart')) }}

with production as (
    select
        product_code,
        batch_number,
        count(*) as total_production_orders,
        sum(actual_qty) as total_actual_qty,
        sum(rejected_qty) as total_rejected_qty,
        avg(yield_pct) as avg_yield_pct,
        avg(scrap_pct) as avg_scrap_pct
    from {{ ref('silver_mes_production_orders') }}
    group by 1, 2
),
quality_tests as (
    select
        product_code,
        batch_number,
        count(*) as total_tests,
        count_if(pass_fail_flag = true) as passed_tests,
        count_if(pass_fail_flag = false) as failed_tests,
        round(cast(count_if(pass_fail_flag = true) as double) / nullif(count(*), 0) * 100, 2) as pass_rate,
        avg(result_value) as avg_result_value,
        avg(deviation_from_mean) as avg_deviation_from_mean,
        count_if(spec_status = 'ABOVE_USL') as above_usl_count,
        count_if(spec_status = 'BELOW_LSL') as below_lsl_count
    from {{ ref('silver_iqms_quality_tests') }}
    group by 1, 2
),
deviations as (
    select
        product_code,
        batch_number,
        count(*) as total_deviations,
        count_if(is_open = true) as open_deviations,
        count_if(severity = 'CRITICAL') as critical_deviation_count,
        count_if(severity = 'MAJOR') as major_deviation_count,
        count_if(severity = 'MINOR') as minor_deviation_count,
        avg(severity_score) as avg_severity_score,
        max(severity_score) as max_severity_score
    from {{ ref('silver_iqms_deviations') }}
    group by 1, 2
)
select
    coalesce(p.product_code, qt.product_code, d.product_code) as product_code,
    coalesce(p.batch_number, qt.batch_number, d.batch_number) as batch_number,
    coalesce(p.total_production_orders, 0) as total_production_orders,
    coalesce(p.total_actual_qty, 0) as total_actual_qty,
    coalesce(p.total_rejected_qty, 0) as total_rejected_qty,
    coalesce(p.avg_yield_pct, 0) as avg_yield_pct,
    coalesce(qt.total_tests, 0) as total_tests,
    coalesce(qt.passed_tests, 0) as passed_tests,
    coalesce(qt.failed_tests, 0) as failed_tests,
    coalesce(qt.pass_rate, 0) as pass_rate,
    coalesce(d.total_deviations, 0) as total_deviations,
    coalesce(d.open_deviations, 0) as open_deviations,
    coalesce(d.critical_deviation_count, 0) as critical_deviation_count,
    coalesce(d.major_deviation_count, 0) as major_deviation_count,
    case
        when d.critical_deviation_count > 0 or d.open_deviations > 0 then 'PENDING'
        when qt.pass_rate is null then 'PENDING'
        when qt.pass_rate >= 95.0 and coalesce(d.critical_deviation_count, 0) = 0 then 'PASS'
        when qt.pass_rate < 80.0 or d.critical_deviation_count > 0 then 'FAIL'
        else 'PENDING'
    end as batch_release_status,
    round(
        least(
            100.0,
            (100.0 - coalesce(qt.pass_rate, 50.0))
            + coalesce(d.critical_deviation_count, 0) * 20.0
            + coalesce(d.major_deviation_count, 0) * 10.0
            + coalesce(d.minor_deviation_count, 0) * 3.0
            + case when coalesce(p.avg_yield_pct, 100) < 85 then 15.0 else 0.0 end
        ),
        2
    ) as overall_risk_score,
    case
        when coalesce(d.critical_deviation_count, 0) > 0 then 'RED'
        when coalesce(d.open_deviations, 0) > 2 or coalesce(qt.pass_rate, 100) < 90 then 'AMBER'
        else 'GREEN'
    end as risk_rag_status,
    current_timestamp as _gold_loaded_at,
    'tpl_lakehouse.dbt' as _data_source
from production p
full outer join quality_tests qt
    on p.product_code = qt.product_code
   and p.batch_number = qt.batch_number
full outer join deviations d
    on coalesce(p.product_code, qt.product_code) = d.product_code
   and coalesce(p.batch_number, qt.batch_number) = d.batch_number
