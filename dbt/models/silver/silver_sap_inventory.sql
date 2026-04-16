{{
  config(
    materialized='incremental',
    unique_key='document_number',
    on_schema_change='append_new_columns',
    tags=['silver', 'sap', 'inventory']
  )
}}

/*
  Silver Layer: SAP ECC Inventory Movements
  Source: bronze.sap_inventory_movements (Kafka CDC from SAP Oracle 9TB)
  ALCOA+ compliant — full movement tracking for inventory audit trail
*/

WITH bronze_source AS (
    SELECT
        _kafka_key,
        TRY(JSON_EXTRACT_SCALAR(_raw_payload, '$.document_number'))         AS document_number,
        TRY(JSON_EXTRACT_SCALAR(_raw_payload, '$.movement_type'))           AS movement_type,
        TRY(JSON_EXTRACT_SCALAR(_raw_payload, '$.movement_description'))    AS movement_description,
        TRY(JSON_EXTRACT_SCALAR(_raw_payload, '$.material_code'))           AS material_code,
        TRY(JSON_EXTRACT_SCALAR(_raw_payload, '$.plant'))                   AS plant,
        TRY(JSON_EXTRACT_SCALAR(_raw_payload, '$.storage_location'))        AS storage_location,
        TRY(CAST(JSON_EXTRACT_SCALAR(_raw_payload, '$.quantity') AS DOUBLE))            AS quantity,
        TRY(JSON_EXTRACT_SCALAR(_raw_payload, '$.uom'))                     AS uom,
        TRY(CAST(JSON_EXTRACT_SCALAR(_raw_payload, '$.posting_date') AS DATE))          AS posting_date,
        TRY(JSON_EXTRACT_SCALAR(_raw_payload, '$.vendor_code'))             AS vendor_code,
        TRY(JSON_EXTRACT_SCALAR(_raw_payload, '$.batch_number'))            AS batch_number,
        TRY(CAST(JSON_EXTRACT_SCALAR(_raw_payload, '$.valuation_amount_inr') AS DOUBLE)) AS valuation_amount_inr,
        _ingested_at,
        _row_hash,
        _kafka_offset,
        _source_system,
        _ingest_year,
        _ingest_month,
        _ingest_day
    FROM {{ source('bronze', 'sap_inventory_movements') }}
    {% if is_incremental() %}
        WHERE _ingested_at > (SELECT MAX(_ingested_at) FROM {{ this }})
    {% endif %}
),

cleaned AS (
    SELECT
        document_number,
        movement_type,
        movement_description,
        material_code,
        plant,
        storage_location,
        ABS(quantity)                                             AS quantity,
        UPPER(TRIM(uom))                                         AS uom,
        posting_date,
        vendor_code,
        batch_number,
        COALESCE(valuation_amount_inr, 0.0)                      AS valuation_amount_inr,
        -- Derived: classify movement_type into business categories
        CASE
            WHEN movement_type IN ('101', '501') THEN 'receipt'
            WHEN movement_type IN ('261')        THEN 'issue'
            WHEN movement_type IN ('311')        THEN 'transfer'
            WHEN movement_type IN ('551')        THEN 'scrap'
            ELSE 'other'
        END                                                      AS movement_category,
        -- Derived: running_balance_flag (+1 for inbound, -1 for outbound)
        CASE
            WHEN movement_type IN ('101', '501', '311') THEN  1
            WHEN movement_type IN ('261', '551')        THEN -1
            ELSE 0
        END                                                      AS running_balance_flag,
        -- Derived: posting month for aggregation
        DATE_TRUNC('month', posting_date)                        AS posting_month,
        -- ALCOA+ metadata
        _ingested_at,
        _row_hash,
        _kafka_offset,
        _source_system,
        _ingest_year,
        _ingest_month,
        _ingest_day,
        CURRENT_TIMESTAMP                                        AS _silver_loaded_at,
        'TPL-Data-Engineering'                                   AS _data_owner
    FROM bronze_source
    WHERE document_number IS NOT NULL
      AND quantity IS NOT NULL
      AND quantity > 0
)

SELECT * FROM cleaned
