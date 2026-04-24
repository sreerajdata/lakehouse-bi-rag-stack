{{ config(materialized='table', properties=iceberg_table_properties('silver', 'silver_quality_events')) }}

with mes_batches as (
    select
        batch_id,
        min(event_ts) as first_event_ts,
        max(event_ts) as last_event_ts,
        arbitrary(product_code) as product_code,
        arbitrary(product_name) as product_name,
        arbitrary(line_id) as line_id,
        count(*) as mes_event_count,
        sum(case when status = 'FAIL' then 1 else 0 end) as mes_fail_count
    from {{ ref('silver_mes_events') }}
    group by 1
),
deviations as (
    select
        cast(deviation_id as varchar) as deviation_id,
        cast(batch_id as varchar) as batch_id,
        cast(product_code as varchar) as product_code,
        cast(deviation_type as varchar) as deviation_type,
        upper(cast(severity as varchar)) as severity,
        cast(description as varchar) as description,
        cast(reported_by as varchar) as reported_by,
        cast(reported_ts as timestamp) as reported_ts,
        upper(cast(status as varchar)) as status,
        cast(resolution_ts as timestamp) as resolution_ts,
        cast(_source as varchar) as _source,
        cast(_ingested_at as timestamp) as _ingested_at,
        cast(_nifi_flow as varchar) as _nifi_flow
    from {{ source('bronze', 'trackwise_deviations') }}
),
joined as (
    select
        deviations.deviation_id,
        deviations.batch_id,
        coalesce(mes_batches.product_code, deviations.product_code) as product_code,
        mes_batches.product_name,
        mes_batches.line_id,
        deviations.deviation_type,
        deviations.severity,
        deviations.description,
        deviations.reported_by,
        deviations.reported_ts,
        deviations.status,
        deviations.resolution_ts,
        mes_batches.first_event_ts,
        mes_batches.last_event_ts,
        mes_batches.mes_event_count,
        mes_batches.mes_fail_count,
        case deviations.severity
            when 'CRITICAL' then 3
            when 'MAJOR' then 2
            when 'MINOR' then 1
            else 0
        end as severity_score,
        case
            when deviations.resolution_ts is null then null
            else date_diff('second', deviations.reported_ts, deviations.resolution_ts) / 3600.0
        end as time_to_resolution_hours,
        deviations.resolution_ts is null as is_open_deviation,
        deviations._source,
        deviations._ingested_at,
        deviations._nifi_flow
    from deviations
    inner join mes_batches
        on deviations.batch_id = mes_batches.batch_id
)
select * from joined
