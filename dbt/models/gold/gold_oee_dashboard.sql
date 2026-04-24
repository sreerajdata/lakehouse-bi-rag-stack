{{ config(materialized='table', properties=iceberg_table_properties('gold', 'gold_oee_dashboard')) }}

select
    machine_id,
    date_trunc('hour', event_ts) as hour_window,
    product_code,
    count(*) as total_events,
    sum(case when status = 'PASS' then 1 else 0 end) as pass_count,
    sum(case when status = 'FAIL' then 1 else 0 end) as fail_count,
    round(sum(case when status = 'PASS' then 1 else 0 end) * 100.0 / nullif(count(*), 0), 2) as quality_rate,
    avg(case when parameter_name = 'temperature' then parameter_value end) as avg_temp,
    avg(case when parameter_name = 'pressure' then parameter_value end) as avg_pressure,
    min(event_ts) as window_start,
    max(event_ts) as window_end
from {{ ref('silver_mes_events') }}
group by 1, 2, 3
