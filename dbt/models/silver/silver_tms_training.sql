{{ config(materialized='table') }}

select
    cast(record_id as varchar) as record_id,
    cast(employee_id as varchar) as employee_id,
    cast(employee_name as varchar) as employee_name,
    cast(department as varchar) as department,
    cast(training_name as varchar) as training_name,
    cast(training_category as varchar) as training_category,
    cast(scheduled_date as date) as scheduled_date,
    cast(completion_date as date) as completion_date,
    cast(score as double) as score,
    upper(cast(status as varchar)) as status,
    cast(trainer_id as varchar) as trainer_id,
    cast(training_mode as varchar) as training_mode,
    cast(validity_months as integer) as validity_months,
    cast(_source as varchar) as _source,
    cast(_ingested_at as timestamp) as _ingested_at
from {{ source('bronze', 'tms_training_completions') }}
where record_id is not null
