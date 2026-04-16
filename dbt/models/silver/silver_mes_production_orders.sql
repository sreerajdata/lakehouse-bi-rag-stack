{{
  config(
    materialized='incremental',
    unique_key='order_id',
    on_schema_change='append_new_columns',
    tags=['silver', 'mes']
  )
}}

WITH bronze_source AS (
    SELECT
        _kafka_key,
        TRY(JSON_EXTRACT_SCALAR(_raw_payload, '$.order_id'))         AS order_id,
        TRY(JSON_EXTRACT_SCALAR(_raw_payload, '$.product_code'))     AS product_code,
        TRY(JSON_EXTRACT_SCALAR(_raw_payload, '$.batch_number'))     AS batch_number,
        TRY(JSON_EXTRACT_SCALAR(_raw_payload, '$.machine_id'))       AS machine_id,
        TRY(JSON_EXTRACT_SCALAR(_raw_payload, '$.operator_id'))      AS operator_id,
        TRY(JSON_EXTRACT_SCALAR(_raw_payload, '$.shift'))            AS shift,
        TRY(JSON_EXTRACT_SCALAR(_raw_payload, '$.plant'))            AS plant,
        TRY(CAST(JSON_EXTRACT_SCALAR(_raw_payload, '$.planned_qty') AS INTEGER))   AS planned_qty,
        TRY(CAST(JSON_EXTRACT_SCALAR(_raw_payload, '$.actual_qty')  AS INTEGER))   AS actual_qty,
        TRY(CAST(JSON_EXTRACT_SCALAR(_raw_payload, '$.rejected_qty') AS INTEGER))  AS rejected_qty,
        TRY(CAST(JSON_EXTRACT_SCALAR(_raw_payload, '$.scrap_percentage') AS DOUBLE)) AS scrap_pct,
        TRY(JSON_EXTRACT_SCALAR(_raw_payload, '$.status'))           AS status,
        TRY(CAST(JSON_EXTRACT_SCALAR(_raw_payload, '$.start_time') AS TIMESTAMP))  AS start_time,
        TRY(CAST(JSON_EXTRACT_SCALAR(_raw_payload, '$.end_time')   AS TIMESTAMP))  AS end_time,
        _ingested_at,
        _row_hash,
        _ingest_year,
        _ingest_month,
        _ingest_day
    FROM {{ source('bronze', 'mes_production_orders') }}
    {% if is_incremental() %}
        WHERE _ingested_at > (SELECT MAX(_ingested_at) FROM {{ this }})
    {% endif %}
),

cleaned AS (
    SELECT
        order_id,
        product_code,
        batch_number,
        machine_id,
        operator_id,
        UPPER(shift)   AS shift,
        plant,
        planned_qty,
        actual_qty,
        rejected_qty,
        COALESCE(scrap_pct, 0.0)                             AS scrap_pct,
        UPPER(TRIM(status))                                  AS status,
        start_time,
        end_time,
        -- Derived fields
        CASE WHEN end_time IS NOT NULL AND start_time IS NOT NULL
             THEN DATE_DIFF('minute', start_time, end_time) END  AS duration_minutes,
        CASE WHEN planned_qty > 0
             THEN ROUND(CAST(actual_qty AS DOUBLE) / planned_qty * 100, 2) END AS yield_pct,
        -- ALCOA+ fields
        _ingested_at,
        _row_hash,
        _ingest_year,
        _ingest_month,
        _ingest_day,
        CURRENT_TIMESTAMP                                    AS _silver_loaded_at,
        'TPL-Data-Engineering'                               AS _data_owner
    FROM bronze_source
    WHERE order_id IS NOT NULL
      AND product_code IS NOT NULL
      AND actual_qty >= 0
      AND planned_qty > 0
)

SELECT * FROM cleaned
