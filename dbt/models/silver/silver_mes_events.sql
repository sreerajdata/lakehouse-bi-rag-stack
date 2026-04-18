{{ config(
    materialized='incremental',
    unique_key='event_id',
    incremental_strategy='merge',
    on_schema_change='append_new_columns'
) }}

with mes as (
    select
        cast(event_id as varchar) as event_id,
        cast(machine_id as varchar) as machine_id,
        cast(batch_id as varchar) as batch_id,
        cast(product_code as varchar) as product_code,
        cast(parameter_name as varchar) as parameter_name,
        cast(parameter_value as double) as parameter_value,
        cast(unit as varchar) as unit,
        cast(operator_id as varchar) as operator_id,
        cast(shift as varchar) as shift,
        cast(event_ts as timestamp) as event_ts,
        upper(coalesce(cast(status as varchar), 'WARNING')) as status,
        cast(_source as varchar) as _source,
        cast(_ingested_at as timestamp) as _ingested_at,
        cast(_nifi_flow as varchar) as _nifi_flow
    from {{ source('bronze', 'mes_events') }}
    {% if is_incremental() %}
    where event_ts >= (
        select coalesce(max(event_ts), timestamp '1900-01-01 00:00:00')
        from {{ this }}
    )
    {% endif %}
),
iqms as (
    select
        cast(order_id as varchar) as order_id,
        cast(product_code as varchar) as product_code,
        cast(batch_id as varchar) as batch_id,
        cast(quantity as integer) as quantity,
        cast(uom as varchar) as uom,
        cast(planned_start as timestamp) as planned_start,
        cast(actual_start as timestamp) as actual_start,
        cast(actual_end as timestamp) as actual_end,
        upper(coalesce(cast(status as varchar), 'PLANNED')) as order_status,
        cast(line_id as varchar) as line_id,
        row_number() over (
            partition by batch_id
            order by coalesce(actual_start, planned_start) desc, order_id desc
        ) as batch_rank
    from {{ source('bronze', 'iqms_orders') }}
),
joined as (
    select
        mes.event_id,
        mes.machine_id,
        mes.batch_id,
        mes.product_code,
        case mes.product_code
            when 'API-100' then 'Paracetamol 500mg'
            when 'API-200' then 'Amoxicillin 250mg'
            when 'API-300' then 'Cetirizine 10mg'
            when 'API-400' then 'Metformin 500mg'
            when 'API-500' then 'Omeprazole 20mg'
            else 'Unknown Product'
        end as product_name,
        mes.parameter_name,
        least(coalesce(mes.parameter_value, 0.0), 1000.0) as parameter_value,
        coalesce(mes.unit, 'UNKNOWN') as unit,
        coalesce(mes.operator_id, 'UNASSIGNED') as operator_id,
        coalesce(mes.shift, 'UNKNOWN') as shift,
        mes.event_ts,
        mes.status,
        iqms.order_id,
        iqms.line_id,
        iqms.quantity as planned_quantity,
        iqms.uom as order_uom,
        iqms.planned_start,
        iqms.actual_start,
        iqms.actual_end,
        iqms.order_status,
        mes._source,
        mes._ingested_at,
        mes._nifi_flow
    from mes
    left join iqms
        on mes.batch_id = iqms.batch_id
       and iqms.batch_rank = 1
),
scored as (
    select
        *,
        case
            when parameter_name = 'temperature' and parameter_value between 15 and 35 then 'PASS'
            when parameter_name = 'pressure' and parameter_value between 0.5 and 10 then 'PASS'
            when parameter_name = 'ph_level' and parameter_value between 6.5 and 8.5 then 'PASS'
            when parameter_name not in ('temperature', 'pressure', 'ph_level') then 'PASS'
            else 'FAIL'
        end as dq_status
    from joined
)
select * from scored
