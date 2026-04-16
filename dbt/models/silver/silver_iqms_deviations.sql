{{
  config(
    materialized='incremental',
    unique_key='deviation_id',
    on_schema_change='append_new_columns',
    tags=['silver', 'iqms', 'deviations']
  )
}}

/*
  Silver Layer: IQMS Deviations (Non-Conformances)
  Source: bronze.iqms_deviations (Kafka CDC from IQMS MSSQL)
  ALCOA+ compliant — full traceability of quality deviations
*/

WITH bronze_source AS (
    SELECT
        _kafka_key,
        TRY(JSON_EXTRACT_SCALAR(_raw_payload, '$.deviation_id'))     AS deviation_id,
        TRY(JSON_EXTRACT_SCALAR(_raw_payload, '$.batch_number'))     AS batch_number,
        TRY(JSON_EXTRACT_SCALAR(_raw_payload, '$.product_code'))     AS product_code,
        TRY(JSON_EXTRACT_SCALAR(_raw_payload, '$.defect_code'))      AS defect_code,
        TRY(JSON_EXTRACT_SCALAR(_raw_payload, '$.severity'))         AS severity,
        TRY(JSON_EXTRACT_SCALAR(_raw_payload, '$.description'))      AS description,
        TRY(JSON_EXTRACT_SCALAR(_raw_payload, '$.detected_by'))      AS detected_by,
        TRY(CAST(JSON_EXTRACT_SCALAR(_raw_payload, '$.detected_at') AS TIMESTAMP)) AS detected_at,
        TRY(JSON_EXTRACT_SCALAR(_raw_payload, '$.status'))           AS status,
        TRY(JSON_EXTRACT_SCALAR(_raw_payload, '$.root_cause'))       AS root_cause,
        _ingested_at,
        _row_hash,
        _kafka_offset,
        _source_system,
        _ingest_year,
        _ingest_month,
        _ingest_day
    FROM {{ source('bronze', 'iqms_deviations') }}
    {% if is_incremental() %}
        WHERE _ingested_at > (SELECT MAX(_ingested_at) FROM {{ this }})
    {% endif %}
),

cleaned AS (
    SELECT
        deviation_id,
        batch_number,
        product_code,
        defect_code,
        UPPER(TRIM(severity))                                    AS severity,
        description,
        detected_by,
        detected_at,
        UPPER(TRIM(status))                                      AS status,
        root_cause,
        -- Derived: numeric severity score for risk aggregation
        CASE UPPER(TRIM(severity))
            WHEN 'CRITICAL' THEN 3
            WHEN 'MAJOR'    THEN 2
            WHEN 'MINOR'    THEN 1
            ELSE 0
        END                                                      AS severity_score,
        -- Derived: days to close (NULL if still open)
        CASE
            WHEN UPPER(TRIM(status)) = 'CLOSED' AND detected_at IS NOT NULL
                THEN DATE_DIFF('day', CAST(detected_at AS DATE), CURRENT_DATE)
            ELSE NULL
        END                                                      AS days_to_close,
        -- Derived: is the deviation still open?
        CASE
            WHEN UPPER(TRIM(status)) IN ('OPEN', 'UNDER_INVESTIGATION') THEN TRUE
            ELSE FALSE
        END                                                      AS is_open,
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
    WHERE deviation_id IS NOT NULL
)

SELECT * FROM cleaned
