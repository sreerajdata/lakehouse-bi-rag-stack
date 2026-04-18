{{ config(materialized='table') }}

with bronze_source as (
    select
        _kafka_key,
        try(json_extract_scalar(_raw_payload, '$.capa_id')) as capa_id,
        try(json_extract_scalar(_raw_payload, '$.capa_type')) as capa_type,
        try(json_extract_scalar(_raw_payload, '$.title')) as title,
        try(json_extract_scalar(_raw_payload, '$.description')) as description,
        try(json_extract_scalar(_raw_payload, '$.source')) as source,
        try(json_extract_scalar(_raw_payload, '$.product_code')) as product_code,
        try(json_extract_scalar(_raw_payload, '$.department')) as department,
        try(json_extract_scalar(_raw_payload, '$.owner')) as owner,
        try(cast(json_extract_scalar(_raw_payload, '$.opened_date') as timestamp)) as opened_date,
        try(cast(json_extract_scalar(_raw_payload, '$.target_close_date') as timestamp)) as target_close_date,
        try(cast(json_extract_scalar(_raw_payload, '$.actual_close_date') as timestamp)) as actual_close_date,
        try(json_extract_scalar(_raw_payload, '$.status')) as status,
        try(cast(json_extract_scalar(_raw_payload, '$.effectiveness_verified') as boolean)) as effectiveness_verified,
        _ingested_at,
        _row_hash,
        _kafka_offset,
        _source_system,
        _ingest_year,
        _ingest_month,
        _ingest_day
    from {{ source('bronze', 'trackwise_capas') }}
),
cleaned as (
    select
        capa_id,
        capa_type,
        title,
        description,
        source,
        product_code,
        department,
        owner,
        cast(opened_date as date) as opened_date,
        cast(target_close_date as date) as target_close_date,
        cast(actual_close_date as date) as actual_close_date,
        upper(trim(status)) as status,
        coalesce(effectiveness_verified, false) as effectiveness_verified,
        case
            when actual_close_date is not null
                then date_diff('day', cast(opened_date as date), cast(actual_close_date as date))
            else date_diff('day', cast(opened_date as date), current_date)
        end as days_open,
        case
            when actual_close_date is null and current_date > cast(target_close_date as date)
                then true
            else false
        end as is_overdue,
        case
            when (
                case
                    when actual_close_date is not null
                        then date_diff('day', cast(opened_date as date), cast(actual_close_date as date))
                    else date_diff('day', cast(opened_date as date), current_date)
                end
            ) > 90 then true
            else false
        end as sla_breach_flag,
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
    where capa_id is not null
)
select * from cleaned
