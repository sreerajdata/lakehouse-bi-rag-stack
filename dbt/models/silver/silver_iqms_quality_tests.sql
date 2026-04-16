{{
  config(
    materialized='incremental',
    unique_key='test_id',
    on_schema_change='append_new_columns',
    tags=['silver', 'iqms', 'quality']
  )
}}

/*
  Silver Layer: IQMS Quality Test Results
  Source: bronze.iqms_quality_tests (Kafka CDC from IQMS MSSQL 2.2TB)
  ALCOA+ compliant — attributable, legible, contemporaneous, original, accurate
*/

WITH bronze_source AS (
    SELECT
        _kafka_key,
        TRY(JSON_EXTRACT_SCALAR(_raw_payload, '$.test_id'))         AS test_id,
        TRY(JSON_EXTRACT_SCALAR(_raw_payload, '$.batch_number'))    AS batch_number,
        TRY(JSON_EXTRACT_SCALAR(_raw_payload, '$.product_code'))    AS product_code,
        TRY(JSON_EXTRACT_SCALAR(_raw_payload, '$.test_type'))       AS test_type,
        TRY(CAST(JSON_EXTRACT_SCALAR(_raw_payload, '$.result_value') AS DOUBLE))  AS result_value,
        TRY(CAST(JSON_EXTRACT_SCALAR(_raw_payload, '$.usl') AS DOUBLE))           AS usl,
        TRY(CAST(JSON_EXTRACT_SCALAR(_raw_payload, '$.lsl') AS DOUBLE))           AS lsl,
        TRY(JSON_EXTRACT_SCALAR(_raw_payload, '$.result'))          AS result,
        TRY(JSON_EXTRACT_SCALAR(_raw_payload, '$.analyst_id'))      AS analyst_id,
        TRY(JSON_EXTRACT_SCALAR(_raw_payload, '$.equipment_id'))    AS equipment_id,
        TRY(CAST(JSON_EXTRACT_SCALAR(_raw_payload, '$.tested_at') AS TIMESTAMP))  AS tested_at,
        TRY(JSON_EXTRACT_SCALAR(_raw_payload, '$.approved_by'))     AS approved_by,
        _ingested_at,
        _row_hash,
        _kafka_offset,
        _source_system,
        _ingest_year,
        _ingest_month,
        _ingest_day
    FROM {{ source('bronze', 'iqms_quality_tests') }}
    {% if is_incremental() %}
        WHERE _ingested_at > (SELECT MAX(_ingested_at) FROM {{ this }})
    {% endif %}
),

cleaned AS (
    SELECT
        test_id,
        batch_number,
        product_code,
        UPPER(TRIM(test_type))                                   AS test_type,
        result_value,
        usl,
        lsl,
        UPPER(TRIM(result))                                      AS result,
        analyst_id,
        equipment_id,
        tested_at,
        approved_by,
        -- Derived fields
        CASE WHEN UPPER(TRIM(result)) = 'PASS' THEN TRUE
             ELSE FALSE
        END                                                      AS pass_fail_flag,
        ROUND((result_value - 100.0) / 100.0, 4)                AS deviation_from_mean,
        CASE
            WHEN result_value > usl THEN 'ABOVE_USL'
            WHEN result_value < lsl THEN 'BELOW_LSL'
            ELSE 'WITHIN_SPEC'
        END                                                      AS spec_status,
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
    WHERE test_id IS NOT NULL
      AND result_value IS NOT NULL
)

SELECT * FROM cleaned
