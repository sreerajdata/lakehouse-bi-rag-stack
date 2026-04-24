{{ config(materialized='table', properties=iceberg_table_properties('gold', 'gold_training_compliance_mart')) }}

select
    department,
    date_trunc('month', scheduled_date) as report_month,
    count(*) as total_trainings,
    count_if(status = 'COMPLETED') as completed_trainings,
    count_if(status = 'OVERDUE') as overdue_trainings,
    round(avg(score), 2) as avg_score,
    round(cast(count_if(status = 'COMPLETED') as double) / nullif(count(*), 0) * 100, 2) as completion_rate_pct,
    case
        when cast(count_if(status = 'COMPLETED') as double) / nullif(count(*), 0) >= 0.95 then 'GREEN'
        when cast(count_if(status = 'COMPLETED') as double) / nullif(count(*), 0) >= 0.80 then 'AMBER'
        else 'RED'
    end as compliance_rag_status,
    current_timestamp as _gold_loaded_at
from {{ ref('silver_tms_training') }}
group by 1, 2
