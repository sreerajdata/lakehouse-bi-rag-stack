{{ config(materialized='table') }}

select
    material_code,
    plant,
    storage_location,
    report_month,
    count(*) as movement_count,
    sum(planned_qty) as planned_stock,
    sum(actual_qty) as actual_stock,
    sum(variance_qty) as variance_qty,
    sum(closing_stock) as closing_stock,
    round(avg(unit_cost), 2) as avg_unit_cost,
    sum(total_cost) as total_cost,
    current_timestamp as _gold_loaded_at
from {{ ref('silver_sap_inventory') }}
group by 1, 2, 3, 4
