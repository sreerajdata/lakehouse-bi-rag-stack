{{ config(materialized='table', properties=iceberg_table_properties('silver', 'silver_iqms_deviations')) }}

with bronze_source as (
    select
        _kafka_key,
        try(json_extract_scalar(_raw_payload, '$.deviation_id')) as deviation_id,
        try(json_extract_scalar(_raw_payload, '$.batch_number')) as batch_number,
        try(json_extract_scalar(_raw_payload, '$.product_code')) as product_code,
        try(json_extract_scalar(_raw_payload, '$.defect_code')) as defect_code,
        try(json_extract_scalar(_raw_payload, '$.severity')) as severity,
        try(json_extract_scalar(_raw_payload, '$.description')) as description,
        try(json_extract_scalar(_raw_payload, '$.detected_by')) as detected_by,
        try(cast(json_extract_scalar(_raw_payload, '$.detected_at') as timestamp)) as detected_at,
        try(json_extract_scalar(_raw_payload, '$.status')) as status,
        try(json_extract_scalar(_raw_payload, '$.root_cause')) as root_cause,
        _ingested_at,
        _row_hash,
        _kafka_offset,
        _source_system,
        _ingest_year,
        _ingest_month,
        _ingest_day
    from {{ source('bronze', 'iqms_deviations') }}
),
cleaned as (
    select
        deviation_id,
        batch_number,
        product_code,
        defect_code,
        upper(trim(severity)) as severity,
        description,
        detected_by,
        detected_at,
        upper(trim(status)) as status,
        root_cause,
        case upper(trim(severity))
            when 'CRITICAL' then 3
            when 'MAJOR' then 2
            when 'MINOR' then 1
            else 0
        end as severity_score,
        case
            when upper(trim(status)) = 'CLOSED' and detected_at is not null
                then date_diff('day', cast(detected_at as date), current_date)
            else null
        end as days_to_close,
        case
            when upper(trim(status)) in ('OPEN', 'UNDER_INVESTIGATION') then true
            else false
        end as is_open,
        _ingested_at,
        _row_hash,
        _kafka_offset,
        _source_system,
        _ingest_year,
        _ingest_month,
        _ingest_day,
        current_timestamp as _silver_loaded_at,
        'Enterprise-Data-Engineering' as _data_owner
    from bronze_source
    where deviation_id is not null
),
deduped as (
    select
        *,
        row_number() over (partition by deviation_id order by _ingested_at desc) as rn
    from cleaned
)
select
    deviation_id,
    batch_number,
    product_code,
    defect_code,
    severity,
    description,
    detected_by,
    detected_at,
    status,
    root_cause,
    severity_score,
    days_to_close,
    is_open,
    _ingested_at,
    _row_hash,
    _kafka_offset,
    _source_system,
    _ingest_year,
    _ingest_month,
    _ingest_day,
    _silver_loaded_at,
    _data_owner
from deduped
where rn = 1
