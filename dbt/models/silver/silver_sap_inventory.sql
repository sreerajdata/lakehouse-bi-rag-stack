{{ config(materialized='table', properties=iceberg_table_properties('silver', 'silver_sap_inventory')) }}

with sap as (
    select
        cast(po_number as varchar) as po_number,
        cast(material_code as varchar) as material_code,
        cast(plant as varchar) as plant,
        cast(storage_location as varchar) as storage_location,
        cast(planned_qty as double) as planned_qty,
        cast(actual_qty as double) as actual_qty,
        cast(uom as varchar) as uom,
        cast(posting_date as date) as posting_date,
        cast(cost_center as varchar) as cost_center,
        cast(total_cost as double) as total_cost,
        cast(currency as varchar) as currency,
        cast(_source as varchar) as _source,
        cast(_ingested_at as timestamp) as _ingested_at,
        cast(_nifi_flow as varchar) as _nifi_flow
    from {{ source('bronze', 'sap_ecc_orders') }}
)
select
    po_number,
    material_code,
    plant,
    storage_location,
    planned_qty,
    actual_qty,
    actual_qty - planned_qty as variance_qty,
    greatest(actual_qty, 0) as closing_stock,
    uom,
    posting_date,
    date_trunc('month', posting_date) as report_month,
    cost_center,
    total_cost,
    round(total_cost / nullif(actual_qty, 0), 2) as unit_cost,
    currency,
    _source,
    _ingested_at,
    _nifi_flow
from sap
