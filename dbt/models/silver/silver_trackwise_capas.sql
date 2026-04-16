{{
  config(
    materialized='incremental',
    unique_key='capa_id',
    on_schema_change='append_new_columns',
    tags=['silver', 'trackwise', 'capa', 'compliance']
  )
}}

/*
  Silver Layer: Trackwise CAPAs (Corrective and Preventive Actions)
  Source: bronze.trackwise_capas (Kafka CDC from Trackwise MSSQL 1.3TB)
  ALCOA+ compliant — 21 CFR Part 11 audit trail for CAPA lifecycle
*/

WITH bronze_source AS (
    SELECT
        _kafka_key,
        TRY(JSON_EXTRACT_SCALAR(_raw_payload, '$.capa_id'))                 AS capa_id,
        TRY(JSON_EXTRACT_SCALAR(_raw_payload, '$.capa_type'))               AS capa_type,
        TRY(JSON_EXTRACT_SCALAR(_raw_payload, '$.title'))                   AS title,
        TRY(JSON_EXTRACT_SCALAR(_raw_payload, '$.description'))             AS description,
        TRY(JSON_EXTRACT_SCALAR(_raw_payload, '$.source'))                  AS source,
        TRY(JSON_EXTRACT_SCALAR(_raw_payload, '$.product_code'))            AS product_code,
        TRY(JSON_EXTRACT_SCALAR(_raw_payload, '$.department'))              AS department,
        TRY(JSON_EXTRACT_SCALAR(_raw_payload, '$.owner'))                   AS owner,
        TRY(CAST(JSON_EXTRACT_SCALAR(_raw_payload, '$.opened_date') AS TIMESTAMP))       AS opened_date,
        TRY(CAST(JSON_EXTRACT_SCALAR(_raw_payload, '$.target_close_date') AS TIMESTAMP)) AS target_close_date,
        TRY(CAST(JSON_EXTRACT_SCALAR(_raw_payload, '$.actual_close_date') AS TIMESTAMP)) AS actual_close_date,
        TRY(JSON_EXTRACT_SCALAR(_raw_payload, '$.status'))                   AS status,
        TRY(CAST(JSON_EXTRACT_SCALAR(_raw_payload, '$.effectiveness_verified') AS BOOLEAN)) AS effectiveness_verified,
        _ingested_at,
        _row_hash,
        _kafka_offset,
        _source_system,
        _ingest_year,
        _ingest_month,
        _ingest_day
    FROM {{ source('bronze', 'trackwise_capas') }}
    {% if is_incremental() %}
        WHERE _ingested_at > (SELECT MAX(_ingested_at) FROM {{ this }})
    {% endif %}
),

cleaned AS (
    SELECT
        capa_id,
        capa_type,
        title,
        description,
        source,
        product_code,
        department,
        owner,
        CAST(opened_date AS DATE)                                AS opened_date,
        CAST(target_close_date AS DATE)                          AS target_close_date,
        CAST(actual_close_date AS DATE)                          AS actual_close_date,
        UPPER(TRIM(status))                                      AS status,
        COALESCE(effectiveness_verified, FALSE)                  AS effectiveness_verified,
        -- Derived: days_open (from opened to close or now)
        CASE
            WHEN actual_close_date IS NOT NULL
                THEN DATE_DIFF('day', CAST(opened_date AS DATE), CAST(actual_close_date AS DATE))
            ELSE DATE_DIFF('day', CAST(opened_date AS DATE), CURRENT_DATE)
        END                                                      AS days_open,
        -- Derived: is_overdue (target passed, not yet closed)
        CASE
            WHEN actual_close_date IS NULL
             AND CURRENT_DATE > CAST(target_close_date AS DATE)
                THEN TRUE
            ELSE FALSE
        END                                                      AS is_overdue,
        -- Derived: SLA breach flag (TRUE if open > 90 days)
        CASE
            WHEN (
                CASE
                    WHEN actual_close_date IS NOT NULL
                        THEN DATE_DIFF('day', CAST(opened_date AS DATE), CAST(actual_close_date AS DATE))
                    ELSE DATE_DIFF('day', CAST(opened_date AS DATE), CURRENT_DATE)
                END
            ) > 90 THEN TRUE
            ELSE FALSE
        END                                                      AS sla_breach_flag,
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
    WHERE capa_id IS NOT NULL
)

SELECT * FROM cleaned
