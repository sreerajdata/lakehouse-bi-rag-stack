{{ config(materialized='table') }}

with bronze_source as (
    select
        _kafka_key,
        try(json_extract_scalar(_raw_payload, '$.order_id')) as order_id,
        try(json_extract_scalar(_raw_payload, '$.product_code')) as product_code,
        try(json_extract_scalar(_raw_payload, '$.batch_number')) as batch_number,
        try(json_extract_scalar(_raw_payload, '$.machine_id')) as machine_id,
        try(json_extract_scalar(_raw_payload, '$.operator_id')) as operator_id,
        try(json_extract_scalar(_raw_payload, '$.shift')) as shift,
        try(json_extract_scalar(_raw_payload, '$.plant')) as plant,
        try(cast(json_extract_scalar(_raw_payload, '$.planned_qty') as integer)) as planned_qty,
        try(cast(json_extract_scalar(_raw_payload, '$.actual_qty') as integer)) as actual_qty,
        try(cast(json_extract_scalar(_raw_payload, '$.rejected_qty') as integer)) as rejected_qty,
        try(cast(json_extract_scalar(_raw_payload, '$.scrap_percentage') as double)) as scrap_pct,
        try(json_extract_scalar(_raw_payload, '$.status')) as status,
        try(cast(json_extract_scalar(_raw_payload, '$.start_time') as timestamp)) as start_time,
        try(cast(json_extract_scalar(_raw_payload, '$.end_time') as timestamp)) as end_time,
        _ingested_at,
        _row_hash,
        _ingest_year,
        _ingest_month,
        _ingest_day
    from {{ source('bronze', 'mes_production_orders') }}
),
cleaned as (
    select
        order_id,
        product_code,
        batch_number,
        machine_id,
        operator_id,
        upper(shift) as shift,
        plant,
        planned_qty,
        actual_qty,
        rejected_qty,
        coalesce(scrap_pct, 0.0) as scrap_pct,
        upper(trim(status)) as status,
        start_time,
        end_time,
        case
            when end_time is not null and start_time is not null
                then date_diff('minute', start_time, end_time)
        end as duration_minutes,
        case
            when planned_qty > 0
                then round(cast(actual_qty as double) / planned_qty * 100, 2)
        end as yield_pct,
        _ingested_at,
        _row_hash,
        _ingest_year,
        _ingest_month,
        _ingest_day,
        current_timestamp as _silver_loaded_at,
        'Enterprise-Data-Engineering' as _data_owner
    from bronze_source
    where order_id is not null
      and product_code is not null
      and actual_qty >= 0
      and planned_qty > 0
)
select * from cleaned
