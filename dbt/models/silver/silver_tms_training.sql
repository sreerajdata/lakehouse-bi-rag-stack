{{
  config(
    materialized='incremental',
    unique_key='record_id',
    on_schema_change='append_new_columns',
    tags=['silver', 'tms', 'training', 'compliance']
  )
}}

/*
  Silver Layer: TMS Training Completions
  Source: bronze.tms_training_completions (Kafka CDC from TMS MSSQL 257GB)
  ALCOA+ compliant — GMP training tracking for 21 CFR Part 11
*/

WITH bronze_source AS (
    SELECT
        _kafka_key,
        TRY(JSON_EXTRACT_SCALAR(_raw_payload, '$.record_id'))          AS record_id,
        TRY(JSON_EXTRACT_SCALAR(_raw_payload, '$.employee_id'))        AS employee_id,
        TRY(JSON_EXTRACT_SCALAR(_raw_payload, '$.employee_name'))      AS employee_name,
        TRY(JSON_EXTRACT_SCALAR(_raw_payload, '$.department'))         AS department,
        TRY(JSON_EXTRACT_SCALAR(_raw_payload, '$.training_name'))      AS training_name,
        TRY(JSON_EXTRACT_SCALAR(_raw_payload, '$.training_category'))  AS training_category,
        TRY(CAST(JSON_EXTRACT_SCALAR(_raw_payload, '$.scheduled_date') AS DATE))   AS scheduled_date,
        TRY(CAST(JSON_EXTRACT_SCALAR(_raw_payload, '$.completion_date') AS DATE))  AS completion_date,
        TRY(CAST(JSON_EXTRACT_SCALAR(_raw_payload, '$.score') AS INTEGER))         AS score,
        TRY(JSON_EXTRACT_SCALAR(_raw_payload, '$.status'))             AS status,
        TRY(JSON_EXTRACT_SCALAR(_raw_payload, '$.trainer_id'))         AS trainer_id,
        TRY(JSON_EXTRACT_SCALAR(_raw_payload, '$.training_mode'))      AS training_mode,
        TRY(CAST(JSON_EXTRACT_SCALAR(_raw_payload, '$.validity_months') AS INTEGER)) AS validity_months,
        _ingested_at,
        _row_hash,
        _kafka_offset,
        _source_system,
        _ingest_year,
        _ingest_month,
        _ingest_day
    FROM {{ source('bronze', 'tms_training_completions') }}
    {% if is_incremental() %}
        WHERE _ingested_at > (SELECT MAX(_ingested_at) FROM {{ this }})
    {% endif %}
),

cleaned AS (
    SELECT
        record_id,
        employee_id,
        employee_name,
        department,
        training_name,
        UPPER(TRIM(training_category))                           AS training_category,
        scheduled_date,
        completion_date,
        score,
        UPPER(TRIM(status))                                      AS status,
        trainer_id,
        training_mode,
        COALESCE(validity_months, 12)                            AS validity_months,
        -- Derived: training_overdue_flag
        CASE
            WHEN UPPER(TRIM(status)) != 'COMPLETED'
             AND scheduled_date < CURRENT_DATE
                THEN TRUE
            ELSE FALSE
        END                                                      AS training_overdue_flag,
        -- Derived: days_since_completion
        CASE
            WHEN completion_date IS NOT NULL
                THEN DATE_DIFF('day', completion_date, CURRENT_DATE)
            ELSE NULL
        END                                                      AS days_since_completion,
        -- Derived: certification_expiry_date (completion + validity_months * 30)
        CASE
            WHEN completion_date IS NOT NULL AND validity_months IS NOT NULL
                THEN DATE_ADD('day', COALESCE(validity_months, 12) * 30, completion_date)
            ELSE NULL
        END                                                      AS certification_expiry_date,
        -- Derived: is certification expired?
        CASE
            WHEN completion_date IS NOT NULL
             AND DATE_ADD('day', COALESCE(validity_months, 12) * 30, completion_date) < CURRENT_DATE
                THEN TRUE
            ELSE FALSE
        END                                                      AS certification_expired,
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
    WHERE record_id IS NOT NULL
)

SELECT * FROM cleaned
