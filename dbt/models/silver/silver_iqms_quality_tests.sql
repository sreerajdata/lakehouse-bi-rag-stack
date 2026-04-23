{{ config(materialized='table') }}

with bronze_source as (
    select
        _kafka_key,
        try(json_extract_scalar(_raw_payload, '$.test_id')) as test_id,
        try(json_extract_scalar(_raw_payload, '$.batch_number')) as batch_number,
        try(json_extract_scalar(_raw_payload, '$.product_code')) as product_code,
        try(json_extract_scalar(_raw_payload, '$.machine_id')) as machine_id,
        try(json_extract_scalar(_raw_payload, '$.test_type')) as test_type,
        try(cast(json_extract_scalar(_raw_payload, '$.result_value') as double)) as result_value,
        try(cast(json_extract_scalar(_raw_payload, '$.usl') as double)) as usl,
        try(cast(json_extract_scalar(_raw_payload, '$.lsl') as double)) as lsl,
        try(json_extract_scalar(_raw_payload, '$.result')) as result,
        try(json_extract_scalar(_raw_payload, '$.analyst_id')) as analyst_id,
        try(json_extract_scalar(_raw_payload, '$.equipment_id')) as equipment_id,
        try(cast(from_iso8601_timestamp(json_extract_scalar(_raw_payload, '$.tested_at')) as timestamp)) as tested_at,
        try(json_extract_scalar(_raw_payload, '$.approved_by')) as approved_by,
        _ingested_at,
        _row_hash,
        _kafka_offset,
        _source_system,
        _ingest_year,
        _ingest_month,
        _ingest_day
    from {{ source('bronze', 'iqms_quality_tests') }}
),
cleaned as (
    select
        test_id,
        batch_number,
        product_code,
        machine_id,
        upper(trim(test_type)) as test_type,
        result_value,
        usl,
        lsl,
        upper(trim(result)) as result,
        analyst_id,
        equipment_id,
        tested_at,
        approved_by,
        case when upper(trim(result)) = 'PASS' then true else false end as pass_fail_flag,
        round((result_value - 100.0) / 100.0, 4) as deviation_from_mean,
        case
            when result_value > usl then 'ABOVE_USL'
            when result_value < lsl then 'BELOW_LSL'
            else 'WITHIN_SPEC'
        end as spec_status,
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
    where test_id is not null
      and result_value is not null
),
deduplicated as (
    select *,
        row_number() over (partition by test_id order by _ingested_at desc) as rn
    from cleaned
)
select * from deduplicated where rn = 1
