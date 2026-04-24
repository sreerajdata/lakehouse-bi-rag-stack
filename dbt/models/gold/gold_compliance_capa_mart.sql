{{ config(materialized='table', properties=iceberg_table_properties('gold', 'gold_compliance_capa_mart')) }}

with capas as (
    select
        capa_id,
        capa_type,
        source as capa_source,
        product_code,
        department,
        owner,
        status,
        cast(opened_date as date) as opened_date,
        cast(target_close_date as date) as target_close_date,
        cast(actual_close_date as date) as actual_close_date,
        effectiveness_verified,
        case
            when actual_close_date is not null
                then date_diff('day', cast(opened_date as date), cast(actual_close_date as date))
            else date_diff('day', cast(opened_date as date), current_date)
        end as days_open,
        case
            when actual_close_date is null and current_date > cast(target_close_date as date)
                then true else false
        end as is_overdue,
        _ingested_at
    from {{ ref('silver_trackwise_capas') }}
),
deviations as (
    select
        product_code,
        severity,
        date_trunc('month', cast(detected_at as date)) as month_year,
        count(*) as deviation_count
    from {{ ref('silver_iqms_deviations') }}
    group by 1, 2, 3
),
capa_summary as (
    select
        product_code,
        department,
        date_trunc('month', opened_date) as report_month,
        count(*) as total_capas,
        count_if(status = 'CLOSED') as closed_capas,
        count_if(status in ('OPEN', 'IN_PROGRESS')) as open_capas,
        count_if(is_overdue = true) as overdue_capas,
        count_if(effectiveness_verified = true) as effectiveness_verified_count,
        avg(days_open) as avg_days_open,
        max(days_open) as max_days_open,
        round(cast(count_if(status = 'CLOSED') as double) / nullif(count(*), 0) * 100, 2) as closure_rate_pct
    from capas
    group by 1, 2, 3
)
select
    cs.*,
    coalesce(d.deviation_count, 0) as linked_deviations_critical,
    case
        when cs.closure_rate_pct >= 90 then 'GREEN'
        when cs.closure_rate_pct >= 70 then 'AMBER'
        else 'RED'
    end as compliance_rag_status,
    current_timestamp as _gold_loaded_at
from capa_summary cs
left join deviations d
    on cs.product_code = d.product_code
   and d.severity = 'CRITICAL'
   and d.month_year = cs.report_month
