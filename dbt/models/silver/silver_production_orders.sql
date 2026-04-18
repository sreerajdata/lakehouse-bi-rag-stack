{{ config(materialized='table') }}

with iqms as (
    select
        cast(order_id as varchar) as order_id,
        cast(product_code as varchar) as product_code,
        cast(batch_id as varchar) as batch_id,
        cast(quantity as double) as quantity,
        cast(uom as varchar) as uom,
        cast(planned_start as timestamp) as planned_start,
        cast(actual_start as timestamp) as actual_start,
        cast(actual_end as timestamp) as actual_end,
        upper(cast(status as varchar)) as status,
        cast(line_id as varchar) as line_id
    from {{ source('bronze', 'iqms_orders') }}
),
sap as (
    select
        replace(cast(material_code as varchar), 'MAT', 'API') as product_code,
        cast(plant as varchar) as plant,
        cast(storage_location as varchar) as storage_location,
        avg(cast(planned_qty as double)) as planned_qty,
        avg(cast(actual_qty as double)) as actual_qty,
        arbitrary(cast(uom as varchar)) as sap_uom,
        max(cast(posting_date as date)) as posting_date,
        arbitrary(cast(cost_center as varchar)) as cost_center,
        avg(cast(total_cost as double)) as total_cost,
        arbitrary(cast(currency as varchar)) as currency
    from {{ source('bronze', 'sap_ecc_orders') }}
    group by 1, 2, 3
),
matched as (
    select
        iqms.order_id,
        iqms.batch_id,
        iqms.product_code,
        iqms.line_id,
        iqms.quantity as iqms_quantity,
        iqms.uom as iqms_uom,
        iqms.planned_start,
        iqms.actual_start,
        iqms.actual_end,
        iqms.status,
        sap.plant,
        sap.storage_location,
        sap.planned_qty,
        sap.actual_qty,
        sap.sap_uom,
        sap.posting_date,
        sap.cost_center,
        sap.total_cost,
        sap.currency,
        row_number() over (
            partition by iqms.order_id
            order by abs(date_diff('day', cast(sap.posting_date as timestamp), iqms.actual_start)), sap.cost_center
        ) as sap_match_rank
    from iqms
    left join sap
        on iqms.product_code = sap.product_code
),
final as (
    select
        order_id,
        batch_id,
        product_code,
        line_id,
        plant,
        storage_location,
        coalesce(planned_qty, iqms_quantity) as planned_qty,
        coalesce(actual_qty, iqms_quantity) as actual_qty,
        coalesce(sap_uom, iqms_uom) as uom,
        planned_start,
        actual_start,
        actual_end,
        status,
        posting_date,
        cost_center,
        total_cost,
        currency,
        round((coalesce(actual_qty, 0) / nullif(coalesce(planned_qty, 0), 0)) * 100, 2) as yield,
        round(coalesce(total_cost, 0) / nullif(coalesce(actual_qty, 0), 0), 2) as cost_per_unit,
        case
            when (coalesce(actual_qty, 0) / nullif(coalesce(planned_qty, 0), 0)) >= 0.95 then 'HIGH'
            when (coalesce(actual_qty, 0) / nullif(coalesce(planned_qty, 0), 0)) >= 0.85 then 'MEDIUM'
            else 'LOW'
        end as production_efficiency
    from matched
    where sap_match_rank = 1
)
select * from final
